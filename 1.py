def on_success_callback(context, **kwargs):
    num_records = get_num_records(**kwargs)
    success_message = "Task succeeded. Spark job completed successfully with {} records!".format(num_records)
    print(success_message)
