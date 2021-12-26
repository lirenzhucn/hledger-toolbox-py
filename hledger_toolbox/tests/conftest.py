import pathlib

import pytest


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
