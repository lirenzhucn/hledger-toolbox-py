import pathlib
import subprocess
import traceback

import pytest
import click.testing

from hledger_toolbox.fidelity import fidelity_import


@pytest.fixture
def sample_fidelity_csv(tmp_path: pathlib.Path) -> pathlib.Path:
    sample_file = tmp_path / "sample.csv"
    sample_text = """
 03/15/2021, YOU SOLD FIDELITY CALIFORNIA MUNICIPAL INCOME (FCTFX) (Cash), FCTFX, FIDELITY CALIFORNIA MUNICIPAL INCOME,Cash,-10,15.00,,,,150,03/16/2021,
 03/08/2021, REVERSE SPLIT R/S FROM 517103404#REOR M0051311040001 LARGO RESOURCES LTD COM NPV (POST REV S (517103602) (Cash), LGO, LARGO RESOURCES LTD COM NPV (POST REV S,Cash,130,,,,,,,
 03/08/2021, REVERSE SPLIT R/S TO 517103602#REOR M0051311040000 LARGO RESOURCES COM NPV ISIN CA51710340 (517103404) (Cash), LGO, LARGO RESOURCES COM NPV ISIN CA51710340,Cash,-1300,,,,,,,
 02/22/2021, YOU BOUGHT CLOSING TRANSACTION CALL (MSFT) MICROSOFT CORP FEB 26 21 $250 (100 SHS) (Cash), -MSFT210226C250, CALL (MSFT) MICROSOFT CORP FEB 26 21 $250 (100 SHS),Cash,1,0.1,,0.04,,-10.04,02/23/2021,
 01/29/2021, DIVIDEND RECEIVED FIDELITY GOVERNMENT MONEY MARKET (SPAXX) (Cash), SPAXX, FIDELITY GOVERNMENT MONEY MARKET,Cash,,,,,,0.1,,
 01/29/2021, REINVESTMENT FIDELITY CALIFORNIA MUNICIPAL INCOME (FCTFX) (Cash), FCTFX, FIDELITY CALIFORNIA MUNICIPAL INCOME,Cash,1.811,13.51,,,,-24.47,,
 01/29/2021, DIVIDEND RECEIVED FIDELITY CALIFORNIA MUNICIPAL INCOME (FCTFX) (Cash), FCTFX, FIDELITY CALIFORNIA MUNICIPAL INCOME,Cash,,,,,,24.47,,
 01/25/2021, Electronic Funds Transfer Paid (Cash), , No Description,Cash,,,,,,-2000,,
 01/21/2021, YOU BOUGHT LARGO RESOURCES COM NPV ISIN CA51710340 (517103404) (Cash), LGO, LARGO RESOURCES COM NPV ISIN CA51710340,Cash,300,1.6,,,,-480,01/25/2021,
 01/13/2021, YOU SOLD MICROSOFT CORP (MSFT) (Cash), MSFT, MICROSOFT CORP,Cash,-15,216,,0.08,,3239.92,01/15/2021,11/15/2019
 01/12/2021, YOU BOUGHT LARGO RESOURCES COM NPV ISIN CA51710340 (517103404) (Cash), LGO, LARGO RESOURCES COM NPV ISIN CA51710340,Cash,900,1.16,,,,-1047.71,01/14/2021,
 01/11/2021, EXPIRED CALL (MSFT) MICROSOFT CORP JAN 08 21 $250 as of 01/08/2021 CALL (MSFT) MICROSOFT CORP JAN 08 21 $250 (100 SHS) (Cash), -MSFT210108C250, CALL (MSFT) MICROSOFT CORP JAN 08 21 $250 (100 SHS),Cash,2,,,,,,,
 01/11/2021, YOU SOLD OPENING TRANSACTION CALL (MSFT) MICROSOFT CORP FEB 26 21 $250 (100 SHS) (Cash), -MSFT210226C250, CALL (MSFT) MICROSOFT CORP FEB 26 21 $250 (100 SHS),Cash,-1,1.15,0.65,0.05,,114.3,01/12/2021,
 01/06/2021, YOU SOLD MICROSOFT CORP (MSFT) (Cash), MSFT, MICROSOFT CORP,Cash,-14,215.6,,0.07,,3018.33,01/08/2021,11/16/2020
 01/06/2021, YOU BOUGHT PROSPECTUS UNDER SEPARATE COVER FIDELITY CALIFORNIA MUNICIPAL INCOME (FCTFX) (Cash), FCTFX, FIDELITY CALIFORNIA MUNICIPAL INCOME,Cash,37.147,13.46,,,,-500,01/07/2021,
 01/05/2021, JOURNALED SPP PURCHASE CREDIT (Cash), , No Description,Cash,,,,,,6531.27,,
 01/04/2021, YOU BOUGHT ESPP### AS OF 12-31-20 MICROSOFT CORP (MSFT) (Cash), MSFT, MICROSOFT CORP,Cash,32.627,200.18,,,,-6531.27,01/05/2021,
    """
    with open(sample_file, "w") as fp:
        fp.write(sample_text)
    return sample_file


def test_import_end_to_end(
    sample_journal: pathlib.Path, sample_fidelity_csv: pathlib.Path
):
    runner = click.testing.CliRunner(env={"LEDGER_FILE": str(sample_journal)})
    result = runner.invoke(
        fidelity_import, [str(sample_fidelity_csv), "-", "-a", "assets:broker"]
    )
    if result.exit_code != 0:
        _, exc_obj, tb = result.exc_info
        traceback.print_tb(tb)
        pytest.fail(
            f"running fidelity import failed with exit code {result.exit_code} "
            f"and exception {exc_obj}"
        )
    with open(sample_journal, "r") as fp:
        journal_text = fp.read()
    journal_text += "\n\n" + result.output
    hledger_proc = subprocess.run(
        ["hledger", "-f-", "bal", "-O", "csv"],
        input=journal_text.encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert (
        hledger_proc.returncode == 0
    ), "hledger parsing of the resulting journal failed"
    expected_hledger_output = '''
"account","balance"
"assets:broker:cash","$14379.930000"
"assets:broker:fctfx:20210106","27.147000 FCTFX"
"assets:broker:fctfx:20210129","1.811000 FCTFX"
"assets:broker:lgo:20210112","90.000000 LGO"
"assets:broker:lgo:20210121","30.000000 LGO"
"assets:broker:msft:20191115","223.000000 MSFT"
"assets:broker:msft:20200305","35.204000 MSFT"
"assets:broker:msft:20201116","50.000000 MSFT"
"assets:broker:msft:20210104","32.627000 MSFT"
"assets:broker:msft210129c255:20201221","-1 MSFTcbabcjCcff"
"assets:transfer","$-4531.270000"
"equity:starting balance","$-66796.110000"
"expenses:investment:trading fees","$0.000040"
"revenues:investment:dividends:fctfx","$-24.470000"
"revenues:investment:dividends:spaxx","$-0.100000"
"revenues:investment:realized long term gain","$-1019.020000"
"revenues:investment:realized short term gain","$-205.445533"
"total","$-58196.485493, 28.958000 FCTFX, 120.000000 LGO, 340.831000 MSFT, -1 MSFTcbabcjCcff"
    '''.strip()
    actual_hledger_output = hledger_proc.stdout.decode().strip()
    assert actual_hledger_output == expected_hledger_output, "hledger output does not match"
    hledger_proc = subprocess.run(
        ["hledger", "-f-", "bal", "-B", "-O", "csv"],
        input=journal_text.encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert (
        hledger_proc.returncode == 0
    ), "hledger parsing of the resulting journal failed"
    expected_hledger_output = '''
"account","balance"
"assets:broker:cash","$14379.930000"
"assets:broker:fctfx:20210106","$365.375524"
"assets:broker:fctfx:20210129","$24.470000"
"assets:broker:lgo:20210112","$1047.709980"
"assets:broker:lgo:20210121","$480.000000"
"assets:broker:msft:20191115","$33017.380000"
"assets:broker:msft:20200305","$6004.080000"
"assets:broker:msft:20201116","$10825.500000"
"assets:broker:msft:20210104","$6531.269989"
"assets:broker:msft210129c255:20201221","$-99.300000"
"assets:transfer","$-4531.270000"
"equity:starting balance","$-66796.110000"
"expenses:investment:trading fees","$0.000040"
"revenues:investment:dividends:fctfx","$-24.470000"
"revenues:investment:dividends:spaxx","$-0.100000"
"revenues:investment:realized long term gain","$-1019.020000"
"revenues:investment:realized short term gain","$-205.445533"
"total","0"
    '''.strip()
    actual_hledger_output = hledger_proc.stdout.decode().strip()
    assert actual_hledger_output == expected_hledger_output, "hledger output does not match"
