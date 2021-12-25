from datetime import datetime
import decimal
import os
import pathlib

import pytest
import pytest_mock

from hledger_toolbox import utils


@pytest.mark.parametrize(
    ["with_numbers", "expected"],
    [
        ("MSFT210514C300", "MSFTcbafbeCdaa"),
        (" -MSFT210514C300", "MSFTcbafbeCdaa"),
        ("-SELB210416C7.5", "SELBcbaebgCh_f"),
    ],
)
def test_map_options_commodity_symbol(with_numbers: str, expected: str):
    actual = utils.map_options_commodity_symbol(with_numbers)
    assert actual == expected


@pytest.fixture
def sample_journal(tmp_path: pathlib.Path) -> pathlib.Path:
    sample_journal_file = tmp_path / "sample.journal"
    sample_text = """
2020-12-31 * Fidelity Opening Balance
    assets:broker:amat:20200327      20 AMAT @ $47.17
    assets:broker:amat:20200406      10 AMAT @ $45.41
    assets:broker:amat:20200420      10 AMAT @ $51.79
    assets:broker:amat:20200706      12 AMAT @ $63.17
    assets:broker:amat:20200904       3 AMAT @ $61.33
    assets:broker:amat:20200909      10 AMAT @ $56.61
    assets:broker:amat:20201208      0.160 AMAT @@ $14.30
    assets:broker:amat210219p75      -100 AMATcbacbkPif @@ $164.30
    assets:broker:gild:20200622      20 GILD @@ $1512.20
    assets:broker:gild:20200706      12 GILD @@ $928.68
    assets:broker:gild:20200904       8 GILD @@ $529.24
    assets:broker:gild:20201016      60 GILD @ $62.22
    assets:broker:gild210115c70      -100 GILDcbabbfCia @@ $21.30
    assets:broker:msft:20191115      238 MSFT @ $148.06
    assets:broker:msft:20200305      35.204 MSFT @@ $6004.08  ; espp:
    assets:broker:msft:20200515      72 MSFT @ $180.53
    assets:broker:msft:20200630      20.554 MSFT @@ $3764.62  ; espp:
    assets:broker:msft:20200817      76 MSFT @ $208.90
    assets:broker:msft:20200930      41.594 MSFT@@ $7873.74  ; espp:
    assets:broker:msft:20201116      64 MSFT @ $216.51
    assets:broker:selb:20200421      500 SELB @ $2.75
    assets:broker:selb:20200422      100 SELB @ $2.74
    assets:broker:selb:20200904      300 SELB @ $2.18
    assets:broker:selb:20201001      666 SELB @ $1.50
    assets:broker:msft210108c250:20201125     -100 MSFTcbabaiCcfa @@ $98.62  ; acquired on 2020-11-25
    assets:broker:msft210129c255:20201221     -100 MSFTcbabcjCcff @@ $99.30  ; acquired on 2020-12-21
    assets:broker:selb210115c5:20201125       -300 SELBcbabbfCf @@ $87.93    ; acquired on 2020-11-25
    assets:broker:selb210219c5:20201222       -700 SELBcbacbjCf @@ $92.19    ; acquired on 2020-12-22
    assets:broker:selb210319c5:20201222       -500 SELBcbadbjCf @@ $171.56   ; acquired on 2020-12-22
    assets:broker:cash               $11895.03
    equity:starting balance
    """
    with open(sample_journal_file, "w") as fp:
        fp.write(sample_text)
        # NOTE: a blank line at the end is important to hledger
        fp.write("\n")
    return sample_journal_file


def test_sample_journal_fixture(sample_journal: pathlib.Path):
    with open(sample_journal, "r") as fp:
        all_content = fp.read()
    assert "Fidelity Opening Balance" in all_content


@pytest.fixture
def lots_manager(
    mocker: pytest_mock.MockerFixture, sample_journal: pathlib.Path
) -> utils.CommodityLotsManager:
    mocker.patch.dict(os.environ, {"LEDGER_FILE": str(sample_journal)})
    lm = utils.CommodityLotsManager()
    return lm


def test_get_lots(lots_manager: utils.CommodityLotsManager):
    lots = lots_manager.get_lots("MSFT", "assets:broker")
    assert len(lots) == 7


def test_trade_lots_long_open(lots_manager: utils.CommodityLotsManager):
    trade_date = datetime.strptime("2021-01-31", "%Y-%m-%d")
    actual = utils.trade_lots(
        lots_manager,
        utils.TradeLotsAccounts(
            base_account="assets:broker",
            short_term_account="revenues:investment:short_term",
            long_term_account="revenues:investment:long_term",
        ),
        date=trade_date,
        commodity="MSFT",
        change_in_quantity=decimal.Decimal(10),
        proceeds_or_costs=decimal.Decimal(-3000),
    )
    assert actual.date == trade_date
    assert len(actual.postings) == 2
    assert (
        actual.postings[0].account == "assets:broker:cash"
        and actual.postings[0].amount.value == -3000
    )
    assert (
        actual.postings[1].account
        == f"assets:broker:msft:{trade_date.strftime('%Y%m%d')}"
        and actual.postings[1].amount.value == 10
        and actual.postings[1].price.price_type == utils.PriceType.UNIT
        and actual.postings[1].price.amount.value == 300
    )


def test_trade_lots_short_open(lots_manager: utils.CommodityLotsManager):
    trade_date = datetime.strptime("2021-01-31", "%Y-%m-%d")
    actual = utils.trade_lots(
        lots_manager,
        utils.TradeLotsAccounts(
            base_account="assets:broker",
            short_term_account="revenues:investment:short_term",
            long_term_account="revenues:investment:long_term",
        ),
        date=trade_date,
        commodity="MSFT210515C400",
        change_in_quantity=decimal.Decimal(-100),
        proceeds_or_costs=decimal.Decimal(180),
    )
    assert actual.date == trade_date
    assert len(actual.postings) == 2
    assert (
        actual.postings[0].account == "assets:broker:cash"
        and actual.postings[0].amount.value == 180
    )
    assert (
        actual.postings[1].account
        == f"assets:broker:msft210515c400:{trade_date.strftime('%Y%m%d')}"
        and actual.postings[1].amount.value == -100
        and actual.postings[1].amount.commodity == "MSFTcbafbfCeaa"
        and actual.postings[1].price.price_type == utils.PriceType.UNIT
        and actual.postings[1].price.amount.value == decimal.Decimal("1.8")
    )


def test_trade_lots_long_close(lots_manager: utils.CommodityLotsManager):
    trade_date = datetime.strptime("2021-01-31", "%Y-%m-%d")
    actual = utils.trade_lots(
        lots_manager,
        utils.TradeLotsAccounts(
            base_account="assets:broker",
            short_term_account="revenues:investment:short_term",
            long_term_account="revenues:investment:long_term",
        ),
        date=trade_date,
        commodity="MSFT",
        change_in_quantity=[
            (decimal.Decimal(-38), datetime.strptime("20191115", "%Y%m%d")),
            (decimal.Decimal(-35), datetime.strptime("20200305", "%Y%m%d")),
        ],
        proceeds_or_costs=decimal.Decimal(300 * (35 + 38)),
    )
    actual_lots = lots_manager.get_lots("MSFT", "assets:broker")
    assert actual_lots[0].quantity == decimal.Decimal("200") and actual_lots[
        1
    ].quantity == decimal.Decimal("0.204")
    assert actual.date == trade_date
    assert len(actual.postings) == 5
    assert actual.postings[0].account == "assets:broker:cash" and actual.postings[
        0
    ].amount.value == 300 * (35 + 38)
    assert (
        actual.postings[1].account == f"assets:broker:msft:20191115"
        and actual.postings[1].amount.value == -38
    )
    assert (
        actual.postings[2].account == f"assets:broker:msft:20200305"
        and actual.postings[2].amount.value == -35
    )
    assert (
        actual.postings[3].account == f"revenues:investment:long_term"
        and "{:.2f}".format(actual.postings[3].amount.value) == "-5773.72"
    )
    assert (
        actual.postings[4].account == f"revenues:investment:short_term"
        and "{:.2f}".format(actual.postings[4].amount.value) == "-4530.71"
    )


def test_trade_lots_long_close_fifo(lots_manager: utils.CommodityLotsManager):
    trade_date = datetime.strptime("2021-01-31", "%Y-%m-%d")
    actual = utils.trade_lots(
        lots_manager,
        utils.TradeLotsAccounts(
            base_account="assets:broker",
            short_term_account="revenues:investment:short_term",
            long_term_account="revenues:investment:long_term",
        ),
        date=trade_date,
        commodity="MSFT",
        change_in_quantity=decimal.Decimal(-35 - 238),
        proceeds_or_costs=decimal.Decimal(160 * (35 + 238)),
    )
    actual_lots = lots_manager.get_lots("MSFT", "assets:broker")
    assert actual_lots[0].quantity == decimal.Decimal("0") and actual_lots[
        1
    ].quantity == decimal.Decimal("0.204")
    assert actual.date == trade_date
    assert len(actual.postings) == 5
    assert actual.postings[0].account == "assets:broker:cash" and actual.postings[
        0
    ].amount.value == 160 * (35 + 238)
    assert (
        actual.postings[1].account == f"assets:broker:msft:20191115"
        and actual.postings[1].amount.value == -238
    )
    assert (
        actual.postings[2].account == f"assets:broker:msft:20200305"
        and actual.postings[2].amount.value == -35
    )
    assert (
        actual.postings[3].account == f"revenues:investment:long_term"
        and "{:.2f}".format(actual.postings[3].amount.value) == "-2841.72"
    )
    assert (
        actual.postings[4].account == f"revenues:investment:short_term"
        and "{:.2f}".format(actual.postings[4].amount.value) == "369.29"
    )


def test_trade_lots_short_close(lots_manager: utils.CommodityLotsManager):
    trade_date = datetime.strptime("2021-01-29", "%Y-%m-%d")
    actual = utils.trade_lots(
        lots_manager,
        utils.TradeLotsAccounts(
            base_account="assets:broker",
            short_term_account="revenues:investment:short_term",
            long_term_account="revenues:investment:long_term",
        ),
        date=trade_date,
        commodity="MSFT210129C255",
        change_in_quantity=decimal.Decimal(100),
        proceeds_or_costs=decimal.Decimal(0),
    )
    actual_lots = lots_manager.get_lots("MSFT210129C255", "assets:broker")
    assert not actual_lots or actual_lots[0].quantity == decimal.Decimal("0")
    assert actual.date == trade_date
    assert len(actual.postings) == 3
    assert (
        actual.postings[0].account == "assets:broker:cash"
        and actual.postings[0].amount.value == 0
    )
    assert (
        actual.postings[1].account == "assets:broker:msft210129c255:20201221"
        and actual.postings[1].amount.value == 100
        and actual.postings[1].price.price_type == utils.PriceType.UNIT
        and actual.postings[1].price.amount.value == decimal.Decimal("0.993")
    )
    assert (
        actual.postings[2].account == "revenues:investment:short_term"
        and actual.postings[2].amount.value == decimal.Decimal("-99.3")
    )
