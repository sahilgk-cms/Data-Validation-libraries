import great_expectations as gx
import pandas as pd
import json
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import ColumnMapMetricProvider, column_condition_partial
from great_expectations.execution_engine import PandasExecutionEngine

# https://docs.greatexpectations.io/docs/core/connect_to_data/dataframes/
print(gx.__version__)
data = {
    "name": ["Alice", "Bob", None, "David"],
    "age": [25, 30, 35, 60],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "david@example.com"]
}

df = pd.DataFrame(data)

context = gx.get_context()


# Convert pandas DataFrame to a Great Expectations DataFrame
pandas_ds = context.data_sources.add_pandas(name="my_data_source")

# --- Register a data asset within this data source ---
data_asset = pandas_ds.add_dataframe_asset(name="my_dataframe_data_asset")

# --- Create a batch definition ---
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    name="my_batch_definition"
)

# --- Pass the dataframe as a batch parameter ---
batch_parameters = {"dataframe": df}
batch = batch_definition.get_batch(batch_parameters=batch_parameters)

# # For single expectation
# expectation = gx.expectations.ExpectColumnValuesToBeBetween(
#     column = "age", max_value=10, min_value=1
# )
#
# validation_results = batch.validate(expectation)
# print(validation_results)
# json_result = validation_results.to_json_dict()
# with open("great_ex_errors_1.json", "w") as f:
#     json.dump(json_result, f, indent=2)


################### CUSTOM EXPECTATION NOT WORKING ###########################################
class ColumnValueLengthBetween(ColumnMapMetricProvider):
    """Custom metric to check if the string length of each value is between min and max."""

    condition_metric_name = "column_values.length_between"

    # The logic to test each cell in the column
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, min_value=3, max_value=100, **kwargs):
        return column.astype(str).str.len().between(min_value, max_value, inclusive="both")


# @gx.expectations.expectation
class ExpectColumnValuesToHaveLengthBetween(ColumnMapExpectation):
    """Expect values in this column to have a string length between a minimum and maximum."""

    map_metric = "column_values.length_between"
    success_keys = ("column", "min_value", "max_value")

    default_kwarg_values = {
        "min_value": 3,
        "max_value": 100,
    }
############################################################################################




# # For multiple expectations
expectations = [
    gx.expectations.ExpectColumnValuesToBeBetween(column="age", min_value=1, max_value=100),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="name"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="email"),
    gx.expectations.ExpectColumnValuesToBeUnique(column="email"),
    gx.expectations.ExpectColumnValuesToMatchRegex(column="email", regex=r".+@.+\..+"),

    #custom expectation not working
   # ExpectColumnValuesToHaveLengthBetween(column="name", min_value=3, max_value=100)
]

#
all_results = []
for exp in expectations:
    result = batch.validate(exp)
    all_results.append(result.to_json_dict())  # convert each result to JSON-safe dict

# --- 5️⃣ Save all validation results into one JSON file ---
with open("great_ex_errors_3.json", "w") as f:
    json.dump(all_results, f, indent=2)


