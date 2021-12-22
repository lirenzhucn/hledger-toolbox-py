from dataclasses import dataclass
from datetime import datetime
import decimal
import enum
import logging
import os
import subprocess
import tempfile
from typing import List, Optional

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

    def __str__(self) -> str:
        res = self.account
        if self.amount is not None:
            res += " " * self.spacing + str(self.amount)
        if self.price is not None:
            res += " " + str(self.price)
        return res


@dataclass
class Transaction:
    date: datetime
    description: str
    postings: List[Posting]
    cleared: bool = True
    indent: int = 4

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
