from datetime import datetime

with open("/data/transactions_report.txt", "a") as f:
    f.write(f"\nProcessed at {datetime.now()}\n")

print("task updated successfully.")