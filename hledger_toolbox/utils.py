import csv
from dataclasses import dataclass, field
from datetime import datetime
import decimal
import enum
import logging
import os
import subprocess
import tempfile
from typing import List, Optional, Tuple

logger = logging.getLogger(os.path.basename(__file__))


@dataclass
class Amount:
    commodity: str
    formatter: str
    value: decimal.Decimal

    def __neg__(self) -> "Amount":
        return Amount(
            commodity=self.commodity, formatter=self.formatter, value=-self.value
        )

    def __str__(self) -> str:
        return self.formatter.format(value=self.value, commodity=self.commodity)

    @classmethod
    def from_dollar_string(cls, value: str):
        value = value.strip().lstrip("$")
        negative = value.startswith("(") and value.endswith(")")
        value = value.lstrip("(").rstrip(")").lstrip("$").replace(",", "")
        decimal_value = decimal.Decimal(value)
        if negative:
            decimal_value = -decimal_value
        return cls(
            commodity="$", value=decimal_value, formatter="{commodity:s}{value:.2f}"
        )

    @classmethod
    def dollar_amount(cls, value):
        return cls(
            commodity="$",
            value=decimal.Decimal(value),
            formatter="{commodity:s}{value:.2f}",
        )


class PriceType(enum.Enum):
    UNIT = 0
    TOTAL = 1


@dataclass
class Price:
    price_type: PriceType
    amount: Amount

    def __str__(self) -> str:
        sign = "@" if self.price_type == PriceType.UNIT else "@@"
        return f"{sign} {self.amount}"


@dataclass
class Posting:
    account: str
    amount: Optional[Amount] = None
    price: Optional[Price] = None
    spacing: int = 6
    tags: List[Tuple[str, str]] = field(default_factory=list)

    def __str__(self) -> str:
        res = self.account
        if self.amount is not None:
            res += " " * self.spacing + str(self.amount)
        if self.price is not None:
            res += " " + str(self.price)
        if self.tags:
            res += "  " + ", ".join(f"{key}: {val}" for key, val in self.tags)
        return res


@dataclass
class Transaction:
    date: datetime
    description: str
    postings: List[Posting]
    cleared: bool = True
    indent: int = 4
    tags: List[Tuple[str, str]] = field(default_factory=list)

    def _set_spacings(self):
        longest_account = max(len(posting.account) for posting in self.postings)
        for posting in self.postings:
            posting.spacing = longest_account - len(posting.account) + 6

    def __str__(self) -> str:
        self._set_spacings()
        res = (
            f"{self.date.strftime('%Y-%m-%d')} "
            f"{'* ' if self.cleared else ''}{self.description}"
        )
        if self.tags:
            res += "  ; " + ", ".join(f"{key}: {val}" for key, val in self.tags)
        for posting in self.postings:
            res += "\n"
            res += " " * self.indent + str(posting)
        return res


def get_raw_text_of_pdf(input_path: str) -> str:
    """Get the raw text of a pdf file

    Parameters
    ----------
    input_path : str
        Path to the input pdf file

    Returns
    -------
    str
        The text content of the pdf file in a string
    """
    if not os.path.isfile(input_path):
        # NOTE: this also protects the subprocess call against some malicious inputs
        raise ValueError("input_path must be an existing file")
    if os.path.splitext(input_path)[1] == ".txt":
        with open(input_path, "r") as fp:
            return fp.read()
    try:
        subprocess.run(["pdftotext", "-v"], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        logger.warning("cannot run `pdftotext -v`")
        raise RuntimeError("cannot find pdftotext on the system")
    with tempfile.TemporaryDirectory() as tmpdir:
        txt_path = os.path.join(
            tmpdir, os.path.splitext(os.path.basename(input_path))[0] + ".txt"
        )
        subprocess.run(
            ["pdftotext", "-layout", input_path, txt_path],
            check=True,
            capture_output=True,
        )
        with open(txt_path, "r") as fp:
            return fp.read()


@dataclass
class CommodityLot:
    date: datetime
    commodity: str
    quantity: decimal.Decimal
    price: Price

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.date.strftime('%Y-%m-%d')} "
            f"{self.quantity:.6f} {self.commodity} {str(self.price)})"
        )


def get_commodity_lots(
    base_account: str,
    commodity_symbol: str,
    *,
    hledger_bin: str = "hledger",
    hledger_file: Optional[str] = None,
):
    """Get the date, quantity, and price

    Parameters
    ----------
    base_account: str
        the base account that holds commodities
    commodity_symbol: str
        the symbol of the commodity to be queried
    hledger_bin: str (optional, keyword only)
        path to the hledger binary; default `hledger`
    hledger_file: str | None (optional, keyword only)
        path to the hledger journal file; default is None and will use the one
        in $LEDGER_FILE environment variable

    Returns
    -------
    List[CommodityLot]
        A list of commodity lots
    """
    commodity_account = f"{base_account}:{commodity_symbol.lower()}"
    cmd_commodity = [hledger_bin]
    if hledger_file is not None:
        cmd_commodity += ["-f", hledger_file]
    cmd_commodity += ["bal", commodity_account, "-O", "csv"]
    logger.info("hledger command to run: %s", " ".join(cmd_commodity))
    process_commodity = subprocess.run(cmd_commodity, check=True, capture_output=True)
    cmd_cost_basis = cmd_commodity + ["-B"]
    logger.info("hledger command to run: %s", " ".join(cmd_cost_basis))
    process_cost_basis = subprocess.run(cmd_cost_basis, check=True, capture_output=True)
    # remove the first line (title) and the last line (total)
    csv_reader_commodity = list(
        csv.reader(process_commodity.stdout.decode().strip().split("\n")[1:-1])
    )
    csv_reader_cost_basis = list(
        csv.reader(process_cost_basis.stdout.decode().strip().split("\n")[1:-1])
    )
    res = []
    for row_commodity, row_cost_basis in zip(
        csv_reader_commodity, csv_reader_cost_basis
    ):
        date = datetime.strptime(
            row_commodity[0][len(commodity_account) + 1 :], "%Y%m%d"
        )
        quantity = decimal.Decimal(
            row_commodity[1].strip()[: -len(commodity_symbol)].strip()
        )
        total_amount = Amount.from_dollar_string(row_cost_basis[1].strip())
        unit_price = Price(
            price_type=PriceType.UNIT,
            amount=Amount(
                commodity=total_amount.commodity,
                formatter=total_amount.formatter,
                value=total_amount.value / quantity,
            ),
        )
        res.append(
            CommodityLot(
                date=date,
                commodity=commodity_symbol,
                quantity=quantity,
                price=unit_price,
            )
        )
    return res
