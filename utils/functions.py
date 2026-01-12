from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def get_widget(name: str, default: str) -> str:
    """
    Safely read a Databricks widget value.

    Returns the widget value if it exists and is non-empty.
    Falls back to `default` and prints a warning if the default is used.
    """
    try:
        value = dbutils.widgets.get(name)
        if value:
            return value
        else:
            print(f"[WARN] Widget '{name}' is empty. Using default: {default}")
            return default
    except NameError:
        # dbutils not defined (e.g., local testing)
        print(f"[WARN] dbutils not available. Using default for '{name}': {default}")
        return default
    except Exception as e:
        print(f"[WARN] Failed to read widget '{name}' ({e}). Using default: {default}")
        return default

def get_date_n_months_ago(n, format="%Y-%m") -> str:
    """
    Return the date `n` months ago in the given format.
    """
    return (datetime.today() - relativedelta(months=n)).strftime(format)

def parse_month_yyyy_mm(month_str: str) -> date:
    """
    Parse a YYYY-MM string into a date corresponding to the
    first day of that month (YYYY-MM-01).
    """
    return datetime.strptime(f"{month_str}-01", "%Y-%m-%d").date()


def compute_month_range(
    end_month: date,
    months_prev: int,
    inclusive: bool = True,
) -> tuple[date, date]:
    """
    Compute a (start_month, end_month) tuple.

    If inclusive=True, the range includes `end_month` and spans
    exactly `months_prev` months.
    """
    if months_prev <= 0:
        raise ValueError("months_prev must be a positive integer")

    offset = months_prev - 1 if inclusive else months_prev
    start_month = end_month - relativedelta(months=offset)
    return start_month, end_month


def read_month_params(
    end_month_default: str = "2025-09",
    months_prev_default: str = "9",
) -> tuple[date, date, int]:
    """
    Read Databricks widgets and return:
      (start_month, end_month, months_prev)

    Widgets expected:
      - end_month (YYYY-MM)
      - months_prev (int, as string)
    """
    end_month_str = get_widget("end_month", end_month_default)
    months_prev = int(get_widget("prev_months", months_prev_default))

    end_month = parse_month_yyyy_mm(end_month_str)
    start_month, _ = compute_month_range(end_month, months_prev)

    return start_month, end_month, months_prev


def generate_month_list(start_month: date, end_month: date) -> list[str]:
    """
    Generate a list of YYYY-MM strings from start_month to end_month (inclusive).
    """
    months = []
    d = start_month
    while d <= end_month:
        months.append(d.strftime("%Y-%m"))
        d += relativedelta(months=1)
    return months