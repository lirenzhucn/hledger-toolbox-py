import pathlib

import pytest


@pytest.fixture
def sample_journal(tmp_path: pathlib.Path) -> pathlib.Path:
    sample_journal_file = tmp_path / "sample.journal"
    sample_text = """
2020-12-31 * Fidelity Opening Balance
    assets:broker:msft:20191115      238 MSFT @ $148.06
    assets:broker:msft:20200305      35.204 MSFT @@ $6004.08  ; espp:
    assets:broker:msft:20201116      64 MSFT @ $216.51
    assets:broker:msft210108c250:20201125     -1 MSFTcbabaiCcfa @@ $98.62  ; acquired on 2020-11-25
    assets:broker:msft210129c255:20201221     -1 MSFTcbabcjCcff @@ $99.30  ; acquired on 2020-12-21
    assets:broker:cash               $11895.03
    equity:starting balance
    """
    with open(sample_journal_file, "w") as fp:
        fp.write(sample_text)
        # NOTE: a blank line at the end is important to hledger
        fp.write("\n")
    return sample_journal_file
