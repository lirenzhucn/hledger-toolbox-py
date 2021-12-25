import bisect
import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import decimal
import enum
import logging
import os
import subprocess
import tempfile
from typing import Dict, Iterable, List, Optional, Tuple, Union

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
            commodity="$", value=decimal_value, formatter="{commodity:s}{value:.6f}"
        )

    @classmethod
    def dollar_amount(cls, value):
        return cls(
            commodity="$",
            value=decimal.Decimal(value),
            formatter="{commodity:s}{value:.6f}",
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

    @property
    def unit_price(self) -> Price:
        if self.price.price_type == PriceType.UNIT:
            return self.price
        else:
            return Price(
                price_type=PriceType.UNIT,
                amount=Amount(
                    commodity=self.price.amount.commodity,
                    formatter=self.price.amount.formatter,
                    value=self.price.amount.value / self.quantity,
                ),
            )

    def __lt__(self, other: "CommodityLot") -> bool:
        return self.date < other.date

    def __le__(self, other: "CommodityLot") -> bool:
        return self.date <= other.date

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
) -> List[CommodityLot]:
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
    commodity_account = f"{base_account}:{commodity_symbol.lower()}:"
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
        date = datetime.strptime(row_commodity[0][len(commodity_account) :], "%Y%m%d")
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
    res.sort()
    return res


class CommodityLotsManager:
    def __init__(self) -> None:
        self._lots: Dict[str, List[CommodityLot]] = {}

    def _update_lots(self, commodity: str, base_account: str):
        if f"{base_account}:{commodity}" not in self._lots:
            self._lots[f"{base_account}:{commodity}"] = get_commodity_lots(
                base_account, commodity
            )

    def get_lot(
        self, commodity: str, base_account: str, lot_date: datetime
    ) -> Optional[CommodityLot]:
        self._update_lots(commodity, base_account)
        lots = self._lots.get(f"{base_account}:{commodity}", [])
        ind = bisect.bisect_left(
            lots,
            CommodityLot(
                date=lot_date,
                commodity=commodity,
                quantity=decimal.Decimal(0),
                price=None,
            ),
        )
        if 0 <= ind < len(lots) and lots[ind].date == lot_date:
            return lots[ind]
        else:
            return None

    def get_lots(self, commodity: str, base_account: str) -> List[CommodityLot]:
        self._update_lots(commodity, base_account)
        return self._lots.get(f"{base_account}:{commodity}", [])

    def add_lot(
        self,
        commodity: str,
        base_account: str,
        date: datetime,
        quantity: decimal.Decimal,
        price: Price,
    ):
        self._update_lots(commodity, base_account)
        bisect.insort(
            self._lots[f"{base_account}:{commodity}"],
            CommodityLot(
                date=date, commodity=commodity, quantity=quantity, price=price
            ),
        )

    def sell_from_lot(
        self,
        commodity: str,
        base_account: str,
        date: datetime,
        quantity: decimal.Decimal,
    ):
        lot = self.get_lot(commodity, base_account, date)
        if lot is None:
            raise ValueError(
                f"unable to find the specified lot {date.strftime('%Y%m%d')}"
            )
        if lot.quantity < quantity:
            raise ValueError(
                "not enough quantity in lot "
                f"{base_account}:{commodity}:{date.strftime('%Y%m%d')}: "
                f"requested {quantity:.6f}; have {lot.quantity:.6f}"
            )
        lot.quantity -= quantity


class _OptionsSymbolMapper:
    _inst: Optional["_OptionsSymbolMapper"] = None

    def __getitem__(self, key: int) -> int:
        if key >= ord("0") and key <= ord("9"):
            return key - ord("0") + ord("a")
        elif key == ord("."):
            return ord("_")
        else:
            return key

    @classmethod
    def inst(cls) -> "_OptionsSymbolMapper":
        if cls._inst is None:
            cls._inst = _OptionsSymbolMapper()
        return cls._inst


def map_options_commodity_symbol(with_numbers: str) -> str:
    return with_numbers.strip().strip("-+").translate(_OptionsSymbolMapper.inst())


@dataclass
class TradeLotsAccounts:
    base_account: str
    short_term_account: str
    long_term_account: str


def _trade_to_close_postings(
    accounts: TradeLotsAccounts,
    date: datetime,
    commodity: str,
    unit_price: decimal.Decimal,
    change_in_quantity: Iterable[decimal.Decimal],
    lots: Iterable[CommodityLot],
    lots_manager: CommodityLotsManager,
) -> List[Posting]:
    res = []
    long_term_gain_loss = decimal.Decimal(0)
    short_term_gain_loss = decimal.Decimal(0)
    for change, lot in zip(change_in_quantity, lots):
        lot_date_str = lot.date.strftime("%Y%m%d")
        if abs(lot.quantity) < abs(change):
            raise ValueError(
                f"lot {accounts.base_account}:{commodity.lower()}:{lot_date_str}"
                " does not have enough quantity: "
                f"requested: {change:.6f}; has {lot.quantity:.6f}"
            )
        if date - lot.date >= timedelta(days=365):
            long_term_gain_loss += change * (unit_price - lot.unit_price.amount.value)
        else:
            short_term_gain_loss += change * (unit_price - lot.unit_price.amount.value)
        res.append(
            Posting(
                account=f"{accounts.base_account}:{commodity.lower()}:{lot_date_str}",
                amount=Amount(
                    commodity=map_options_commodity_symbol(commodity),
                    formatter="{value:.6f} {commodity:s}",
                    value=change,
                ),
                price=lot.unit_price,
            )
        )
        lots_manager.sell_from_lot(commodity, accounts.base_account, lot.date, -change)
    if long_term_gain_loss != 0:
        res.append(
            Posting(
                account=accounts.long_term_account,
                amount=Amount.dollar_amount(long_term_gain_loss),
            )
        )
    if short_term_gain_loss != 0:
        res.append(
            Posting(
                account=accounts.short_term_account,
                amount=Amount.dollar_amount(short_term_gain_loss),
            )
        )
    return res


def trade_lots(
    lots_manager: CommodityLotsManager,
    accounts: TradeLotsAccounts,
    date: datetime,
    commodity: str,
    change_in_quantity: Union[
        decimal.Decimal, Iterable[Tuple[decimal.Decimal, datetime]]
    ],
    proceeds_or_costs: decimal.Decimal,
) -> Transaction:
    """Generate a lots-aware transaction that reflects one commodity trade

    Parameters
    ----------
    lots_manager: CommodityLotsManager
        a CommodityLotsManager object that maintains lots info
    accounts: TradeLotsAccounts
        which accounts to post to for various purposes
    date: datetime.datetime
        the date of the trade
    commodity: str
        the commodity symbol *before* mapping
    change_in_quantity: Union[
        decimal.Decimal, Iterable[Tuple[decimal.Decimal, datetime]]
    ]
        change of commodity quantity; either one number or a list of tuples with
        numbers and lot dates
    proceeds_or_costs: decimal.Decimal
        proceeds or costs of the trade

    Returns
    -------
    Transaction
        a `Transaction` object with appropriate postings
    """
    postings = [
        Posting(
            account=f"{accounts.base_account}:cash",
            amount=Amount.dollar_amount(proceeds_or_costs),
        )
    ]
    if isinstance(change_in_quantity, decimal.Decimal):
        lots = lots_manager.get_lots(commodity, accounts.base_account)
        unit_price = abs(proceeds_or_costs / change_in_quantity)
        if not lots or lots[0].quantity * change_in_quantity >= 0:
            # this is a opening trade
            price = Price(
                price_type=PriceType.UNIT, amount=Amount.dollar_amount(unit_price)
            )
            postings.append(
                Posting(
                    account=f"{accounts.base_account}:{commodity.lower()}:"
                    + date.strftime("%Y%m%d"),
                    amount=Amount(
                        commodity=map_options_commodity_symbol(commodity),
                        formatter="{value:.6f} {commodity:s}",
                        value=change_in_quantity,
                    ),
                    price=price,
                )
            )
            lots_manager.add_lot(
                commodity, accounts.base_account, date, change_in_quantity, price
            )
        else:
            # this is a FIFO closing trade
            used_changes = []
            used_lots = []
            i = 0
            remainder = change_in_quantity
            while remainder * change_in_quantity > 0 and i < len(lots):
                if lots[i].quantity == 0:
                    continue
                used_changes.append(
                    -lots[i].quantity
                    if abs(remainder) > abs(lots[i].quantity)
                    else remainder
                )
                used_lots.append(lots[i])
                remainder += lots[i].quantity
                i += 1
            postings.extend(
                _trade_to_close_postings(
                    accounts,
                    date,
                    commodity,
                    unit_price,
                    used_changes,
                    used_lots,
                    lots_manager,
                )
            )
    else:
        # change_in_quantity is an iterable with specific lots...
        # must be a closing trade!
        total_quantity = sum(q for q, _ in change_in_quantity)
        unit_price = abs(proceeds_or_costs / total_quantity)
        postings.extend(
            _trade_to_close_postings(
                accounts,
                date,
                commodity,
                unit_price,
                [q for q, _ in change_in_quantity],
                [
                    lots_manager.get_lot(commodity, accounts.base_account, d)
                    for _, d in change_in_quantity
                ],
                lots_manager,
            )
        )
    return Transaction(date=date, description="", postings=postings)
