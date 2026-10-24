# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

@DBAcademyHelper.add_method
def validate_traffic_query(self,query):
    """
    Validates the query by checking its status, name, and sink description.

    Parameters:
    - query: The streaming query object to validate.

    Returns:
    - None: Prints validation results.
    """
    # Check if the query is active
    if query.isActive:
        print("Test Passed: The query is active.")
    else:
        print("Test Failed: The query is not active.")
        return
    
    # Check the query name
    expected_name = "active_users_by_traffic"
    if query.name == expected_name:
        print(f"Test Passed: The query name is \"{expected_name}\".")
    else:
        print(f"Test Failed: The query name is \"{query.name}\" instead of \"{expected_name}\".")
        return
    
    # Check the sink description format
    expected_sink = "MemorySink"
    actual_sink = query.lastProgress.get("sink", {}).get("description", None)
    if actual_sink == expected_sink:
        print(f"Test Passed: The format is \"{expected_sink}\".")
    else:
        print(f"Test Failed: The format is \"{actual_sink}\" instead of \"{expected_sink}\".")
