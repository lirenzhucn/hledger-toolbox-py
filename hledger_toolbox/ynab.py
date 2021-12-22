import bisect
import csv
from datetime import datetime
import decimal
import dbm.gnu
import json
import logging
import os
from typing import Any, Dict, List, Optional, Type, Tuple

import click
import pydantic
import requests


class CurrencyFormat(pydantic.BaseModel):
    iso_code: str
    example_format: str
    decimal_digits: int
    decimal_separator: str
    symbol_first: bool
    group_separator: str
    currency_symbol: str
    display_symbol: bool


class DeletableWithId(pydantic.BaseModel):
    id: str
    deleted: bool


class Account(DeletableWithId):
    name: str
    type: str
    on_budget: bool
    closed: bool
    note: Optional[str]
    balance: int
    cleared_balance: int
    transfer_payee_id: str
    direct_import_linked: bool
    direct_import_in_error: bool


class Budget(pydantic.BaseModel):
    id: str
    name: str
    last_modified_on: datetime
    first_month: str
    last_month: str
    date_format: Dict[str, str]
    currency_format: CurrencyFormat
    accounts: Optional[List[Account]] = None


class Payee(DeletableWithId):
    name: str
    transfer_account_id: Optional[str] = None


class Category(DeletableWithId):
    category_group_id: str
    name: str
    hidden: bool
    budgeted: int
    activity: int
    balance: int
    goal_target: int
    goal_percentage_complete: Optional[int] = None
    goal_under_funded: Optional[int] = None
    goal_overall_funded: Optional[int] = None
    goal_overall_left: Optional[int] = None
    goal_months_to_budget: Optional[int] = None
    goal_type: Optional[str] = None
    goal_creation_month: Optional[str] = None
    goal_target_month: Optional[str] = None
    note: Optional[str] = None
    original_category_group_id: Optional[str] = None


class CategoryGroup(DeletableWithId):
    name: str
    hidden: bool
    categories: List[Category]


class SubTransaction(DeletableWithId):
    transaction_id: str
    amount: int
    payee_id: Optional[str] = None
    payee_name: Optional[str] = None
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    transfer_account_id: Optional[str] = None
    transfer_transaction_id: Optional[str] = None
    memo: Optional[str] = None


class Transaction(DeletableWithId):
    date: str
    amount: int
    cleared: str  # cleared, uncleared, reconciled
    approved: bool
    account_id: str
    account_name: str
    subtransactions: List[SubTransaction]
    flag_color: Optional[str] = None
    payee_id: Optional[str] = None
    category_id: Optional[str] = None
    payee_name: Optional[str] = None
    category_name: Optional[str] = None
    transfer_account_id: Optional[str] = None
    transfer_transaction_id: Optional[str] = None
    memo: Optional[str] = None
    matched_transaction_id: Optional[str] = None
    import_id: Optional[str] = None


class TransactionTags(pydantic.BaseModel):
    type_: Optional[str] = None
    category_group: Optional[str] = None
    category: Optional[str] = None


class YnabApiSettings(pydantic.BaseModel):
    api_base_url: str
    api_token: str


class Settings(pydantic.BaseModel):
    ynab_api_settings: YnabApiSettings
    budget_name: str
    starting_balance_account: str
    transfer_account: str
    account_map: Dict[str, str]

    @classmethod
    def from_settings_file(cls, settings_file_path: str) -> "Settings":
        with open(settings_file_path, "r") as settings_file:
            settings = cls(**json.load(settings_file))
        return settings


def _init_db(base_dir: str, budget_id: str) -> Any:
    return dbm.gnu.open(os.path.join(base_dir, budget_id), "c")


def _category_is_inflow(category: str) -> bool:
    return category.lower() == "inflow: ready to assign"


def _get_transaction_tags(memo: str) -> Tuple[TransactionTags, str]:
    TYPE_PREFIX = "#type="
    CATEGORY_PREFIX = "#category="
    type_ = None
    category_group = None
    category = None
    desc = []
    if memo:
        for word in memo.split():
            if word.startswith(TYPE_PREFIX):
                type_ = word[len(TYPE_PREFIX) :]
            elif word.startswith(CATEGORY_PREFIX):
                category_group, category = [
                    i.replace("_", " ")
                    for i in word[len(CATEGORY_PREFIX) :].split(":", 2)
                ]
            else:
                desc.append(word)
    return TransactionTags(
        type_=type_, category_group=category_group, category=category
    ), " ".join(desc)


class YnabBudgetData:
    def __init__(
        self, ynab_api_settings: YnabApiSettings, budget_name: str, db_base_dir: str
    ) -> None:
        self._settings = ynab_api_settings
        self._budget_name = budget_name
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.INFO)
        self._accounts = []
        self._payees = []
        self._category_groups = []
        self._transactions = []
        # fetch budget on init
        self._budget = self._get_budget()
        if self._budget is None:
            print(f"unable to obtain a budget that matches {self._budget_name}")
            return
        self._db = _init_db(db_base_dir, self.budget_id)
        # server knowledge numbers
        self._sk_accounts = self._db.get("sk_accounts", b"").decode()
        self._sk_payees = self._db.get("sk_payees", b"").decode()
        self._sk_category_groups = self._db.get("sk_category_groups", b"").decode()
        self._sk_transactions = self._db.get("sk_transactions", b"").decode()

    @property
    def budget_id(self) -> Optional[str]:
        if self._budget is not None:
            return self._budget.id
        return None

    @classmethod
    def _get_auth_headers(cls, api_token: str) -> dict:
        return {"Authorization": f"Bearer {api_token}"}

    @classmethod
    def _get_item_list(
        cls,
        api_settings: YnabApiSettings,
        budget_id: str,
        item_type_name: str,
        item_type: Type[DeletableWithId],
        payload_key: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        server_knowledge: Optional[str] = None,
    ) -> Tuple[List[DeletableWithId], str]:
        if payload_key is None:
            payload_key = item_type_name
        res = []
        url = f"{api_settings.api_base_url}/budgets/{budget_id}/{item_type_name}"
        if server_knowledge is not None:
            url += f"?last_knowledge_of_server={server_knowledge}"
        resp = requests.get(url, headers=cls._get_auth_headers(api_settings.api_token))
        resp.raise_for_status()
        server_knowledge = str(resp.json()["data"]["server_knowledge"])
        for data_dict in resp.json()["data"][payload_key]:
            try:
                data = item_type(**data_dict)
            except (ValueError, TypeError) as exc:
                if logger is not None:
                    logger.warning(
                        f"cannot decode {item_type_name} data: %s; error: %s",
                        json.dumps(data_dict),
                        str(exc),
                    )
                continue
            res.append(data)
        return res, server_knowledge

    @classmethod
    def _write_items(
        cls,
        db: Any,
        items: List[DeletableWithId],
        key_prefix: str,
        server_knowledge_key: str,
        server_knowledge: int,
        logger: Optional[logging.Logger] = None,
    ):
        if logger is None:
            logger = logging.getLogger("_write_items")
        # update server knowledge regardless
        if server_knowledge_key:
            db[server_knowledge_key] = str(server_knowledge)
        for item in items:
            key = f"{key_prefix}{item.id}"
            if item.deleted:
                if db.get(key, None) is not None:
                    logger.info("DELETE item with key %s", key)
                    del db[key]
            else:
                logger.info(
                    "%s item with key %s",
                    "UPDATE" if db.get(key, None) is not None else "CREATE",
                    key,
                )
                db[key] = json.dumps(item.dict())

    def _get_budget(self) -> Optional[Budget]:
        resp = requests.get(
            f"{self._settings.api_base_url}/budgets",
            headers=self._get_auth_headers(self._settings.api_token),
        )
        resp.raise_for_status()
        for budget_dict in resp.json()["data"]["budgets"]:
            try:
                budget = Budget(**budget_dict)
            except (ValueError, TypeError) as exc:
                self._logger.warning(
                    "cannot decode budget data: %s; error: %s",
                    json.dumps(budget_dict),
                    str(exc),
                )
                continue
            if budget.name == self._budget_name:
                self._logger.info(f"budget name [{budget.name}] was a match")
                return budget
        else:
            return None

    def _get_accounts(
        self, budget_id: str, server_knowledge: Optional[str] = None
    ) -> Tuple[List[Account], str]:
        return self._get_item_list(
            self._settings,
            budget_id,
            "accounts",
            Account,
            logger=self._logger,
            server_knowledge=server_knowledge,
        )

    def _get_payees(
        self, budget_id: str, server_knowledge: Optional[str] = None
    ) -> Tuple[List[Payee], str]:
        return self._get_item_list(
            self._settings,
            budget_id,
            "payees",
            Payee,
            logger=self._logger,
            server_knowledge=server_knowledge,
        )

    def _get_category_groups(
        self, budget_id: str, server_knowledge: Optional[str] = None
    ) -> Tuple[List[CategoryGroup], str]:
        return self._get_item_list(
            self._settings,
            budget_id,
            "categories",
            CategoryGroup,
            payload_key="category_groups",
            logger=self._logger,
            server_knowledge=server_knowledge,
        )

    def _get_transactions(
        self, budget_id: str, server_knowledge: Optional[str] = None
    ) -> Tuple[List[Transaction], str]:
        return self._get_item_list(
            self._settings,
            budget_id,
            "transactions",
            Transaction,
            logger=self._logger,
            server_knowledge=server_knowledge,
        )

    def fetch_data(self):
        accounts, self._sk_accounts = self._get_accounts(
            self.budget_id, self._sk_accounts
        )
        payees, self._sk_payees = self._get_payees(self.budget_id, self._sk_payees)
        category_groups, self._sk_category_groups = self._get_category_groups(
            self.budget_id, self._sk_category_groups
        )
        categories = []
        for group in category_groups:
            categories.extend(group.categories)
        transactions, self._sk_transactions = self._get_transactions(
            self.budget_id, self._sk_transactions
        )
        transactions = sorted(
            transactions, key=lambda t: datetime.strptime(t.date, "%Y-%m-%d")
        )
        # write data
        self._write_items(
            self._db,
            accounts,
            "accounts-",
            "sk_accounts",
            self._sk_accounts,
            logger=self._logger,
        )
        self._write_items(
            self._db,
            payees,
            "payees-",
            "sk_payees",
            self._sk_payees,
            logger=self._logger,
        )
        self._write_items(
            self._db,
            category_groups,
            "category_groups-",
            "sk_category_groups",
            self._sk_category_groups,
            logger=self._logger,
        )
        self._write_items(
            self._db, categories, "categories-", "sk_categories", 0, logger=self._logger
        )
        self._write_items(
            self._db,
            transactions,
            "transactions-",
            "sk_transactions",
            self._sk_transactions,
            logger=self._logger,
        )

    def _build_transaction_lut(self) -> Dict[int, List[str]]:
        transaction_lut = dict()
        for key in self._db.keys():
            if key.decode().startswith("transactions-"):
                transaction = Transaction(**json.loads(self._db[key].decode()))
                year = datetime.strptime(transaction.date, "%Y-%m-%d").year
                if year not in transaction_lut:
                    transaction_lut[year] = []
                bisect.insort(transaction_lut[year], (transaction.date, key.decode()))
        return transaction_lut

    def _transaction_to_rows(
        self,
        transaction: Transaction,
        account_map: Dict[str, str],
        transfer_account: str,
        starting_balance_account: str,
    ) -> List[List[str]]:
        if not transaction.subtransactions:
            account = Account(
                **json.loads(self._db[f"accounts-{transaction.account_id}"].decode())
            )
            payee = (
                Payee(**json.loads(self._db[f"payees-{transaction.payee_id}"].decode()))
                if transaction.payee_id
                else None
            )
            category = (
                Category(
                    **json.loads(
                        self._db[f"categories-{transaction.category_id}"].decode()
                    )
                )
                if transaction.category_id
                else None
            )
            category_group = (
                CategoryGroup(
                    **json.loads(
                        self._db[
                            f"category_groups-{category.category_group_id}"
                        ].decode()
                    )
                )
                if category is not None
                else None
            )
            if transaction.memo is None:
                transaction.memo = ""
            tags, memo = _get_transaction_tags(transaction.memo)
            is_starting_balance = payee and payee.name.lower().strip().startswith(
                "starting balance"
            )
            is_transfer = payee and payee.name.lower().strip().startswith("transfer")
            #
            if account.name not in account_map:
                self._logger.warning(
                    "account (%s, %s) not found in map; transaction %s is ignored",
                    account.name,
                    account.id,
                    transaction.id,
                )
                return []
            account1 = account_map[account.name]
            account2 = ""
            if is_transfer:
                account2 = transfer_account
            elif is_starting_balance:
                account2 = starting_balance_account
            else:
                if tags.type_ is None:
                    tags.type_ = (
                        "revenues"
                        if category is not None and _category_is_inflow(category.name)
                        else "expenses"
                    )
                if tags.type_ == "revenue" or tags.type_ == "revenues":
                    tags.type_ = "revenues"
                    tags.category_group = "income"
                    tags.category = payee.name if payee else "Unknown Payee"
                elif tags.type_ == "investment" or tags.type_ == "investments":
                    tags.type_ = "revenues"
                    tags.category_group = "investment"
                    tags.category = payee.name if payee else "Unknown Payee"
                elif tags.type_ == "expenses":
                    if tags.category_group is None:
                        if category_group:
                            tags.category_group = category_group.name
                        else:
                            self._logger.warn(
                                "unable to parse transaction: %s", transaction.id
                            )
                            return []
                    if tags.category is None:
                        if category:
                            tags.category = category.name
                        else:
                            self._logger.warn(
                                "unable to parse transaction: %s", transaction.id
                            )
                            return []
                else:
                    self._logger.warn(
                        "invalid transaction type %s for transaction %s",
                        tags.type_,
                        transaction.id,
                    )
                    return []
                account2 = f"{tags.type_}:{tags.category_group}:{tags.category}"
            desc = []
            if payee and payee.name:
                desc.append(payee.name)
            if memo:
                desc.append(memo)
            amount = decimal.Decimal(transaction.amount) / 1000
            return [
                [transaction.date, account1, account2, " | ".join(desc), str(amount)]
            ]
        else:
            agg_list = []
            # this is a split
            for sub in transaction.subtransactions:
                temp = Transaction(
                    id=sub.id,
                    deleted=False,
                    date=transaction.date,
                    amount=sub.amount,
                    cleared=transaction.cleared,
                    approved=transaction.approved,
                    account_id=transaction.account_id,
                    account_name=transaction.account_name,
                    subtransactions=[],
                    flag_color=transaction.flag_color,
                    payee_id=sub.payee_id,
                    category_id=sub.category_id,
                    payee_name=sub.payee_name,
                    category_name=sub.category_name,
                    transfer_account_id=sub.transfer_account_id,
                    transfer_transaction_id=sub.transfer_transaction_id,
                    memo=sub.memo,
                )
                agg_list.append(
                    self._transaction_to_rows(
                        temp, account_map, transfer_account, starting_balance_account
                    )[0]
                )
            return agg_list

    def write_csv_files(
        self,
        output_path: str,
        account_map: Dict[str, str],
        transfer_account: str,
        starting_balance_account: str,
    ):
        # build a year-transaction LUT
        transaction_lut = self._build_transaction_lut()
        for year in transaction_lut.keys():
            year_dir = os.path.join(output_path, str(year))
            os.makedirs(year_dir, exist_ok=True)
            with open(os.path.join(year_dir, "ynab_data.csv"), "w") as output_file:
                csv_writer = csv.writer(output_file)
                for _, key in transaction_lut[year]:
                    transaction = Transaction(**json.loads(self._db[key].decode()))
                    rows = self._transaction_to_rows(
                        transaction,
                        account_map,
                        transfer_account,
                        starting_balance_account,
                    )
                    csv_writer.writerows(rows)


@click.command(name="import")
@click.argument("output_file", type=click.Path())
@click.option(
    "-p/-n",
    "--pull-data/--no-pull-data",
    default=True,
    help="pull or do not pull data from YNAB API",
)
@click.option(
    "-s",
    "--settings",
    type=click.Path(exists=True),
    required=True,
    help="path to the settings file",
)
@click.option(
    "-d",
    "--db-dir",
    type=click.Path(exists=True),
    required=True,
    help="path to the base dir of local databases",
)
def ynab_import(output_file: str, pull_data: bool, settings: str, db_dir: str):
    """Import YNAB data via its API to OUTPUT_FILE."""
    settings = Settings.from_settings_file(settings)
    # NOTE: budget is fetched in YnabBudgetData.__init__()
    ynab_data = YnabBudgetData(
        settings.ynab_api_settings, settings.budget_name, db_dir
    )
    if ynab_data.budget_id is None:
        print(f"failed to obtain the budget with the name {settings.budget_name}")
        return
    if pull_data:
        ynab_data.fetch_data()
    ynab_data.write_csv_files(
        output_file,
        settings.account_map,
        settings.transfer_account,
        settings.starting_balance_account,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ynab_import()
