# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

@DBAcademyHelper.add_method
def validate_coupon_sales_query(self,coupon_sales_query, output_path, checkpoint_path):
    # Check if the coupon sales query is active
    if coupon_sales_query.isActive:
        print("Coupon sales query is active.")
    else:
        print("Coupon sales query is not active.")

    # Check for files in the output directory
    try:
        output_files = dbutils.fs.ls(output_path)
        if len(output_files) > 0:
            print(f"Found at least one file in {output_path}.")
        else:
            print(f"No files found in {output_path}.")
    except Exception as e:
        print(f"Error accessing {output_path}: {e}")

    # Check for files in the checkpoint directory
    try:
        checkpoint_files = dbutils.fs.ls(checkpoint_path)
        if len(checkpoint_files) > 0:
            print(f"Found at least one file in {checkpoint_path}.")
        else:
            print(f"No files found in {checkpoint_path}.")
    except Exception as e:
        print(f"Error accessing {checkpoint_path}: {e}")
