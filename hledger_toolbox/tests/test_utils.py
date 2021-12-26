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
    assert len(lots) == 3


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
        change_in_quantity=decimal.Decimal(-1),
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
        and actual.postings[1].amount.value == -1
        and actual.postings[1].amount.commodity == "MSFTcbafbfCeaa"
        and actual.postings[1].price.price_type == utils.PriceType.UNIT
        and actual.postings[1].price.amount.value == decimal.Decimal("180")
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
        change_in_quantity=decimal.Decimal(1),
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
        and actual.postings[1].amount.value == 1
        and actual.postings[1].price.price_type == utils.PriceType.UNIT
        and actual.postings[1].price.amount.value == decimal.Decimal("99.3")
    )
    assert (
        actual.postings[2].account == "revenues:investment:short_term"
        and actual.postings[2].amount.value == decimal.Decimal("-99.3")
    )
