# Find the maximum day_rk value for each counterparty_id
max_day_rk_df = (
    raw_interface
    .withColumn("max_day_rk", F.row_number().over(max_day_rk_window))
    .filter("max_day_rk = 1")
    .select("counterparty_id", "day_rk", "postcrm", "precrm", "max_day_rk")
)

# Join the correct_records DataFrame with max_day_rk_df based on conditions
updated_raw_interface = (
    raw_interface
    .join(
        F.broadcast(correct_records),
        (raw_interface["counterparty_id"] == correct_records["cis_code"]) &
        (raw_interface["postcrm"] == correct_records["definitive_pd"]) &
        (raw_interface["precrm"] == correct_records["definitive_pd"]) &
        (raw_interface["day_rk"] == correct_records["last_grading_date"]),
        "left_outer"
    )
    .join(
        max_day_rk_df,
        (raw_interface["counterparty_id"] == max_day_rk_df["counterparty_id"]) &
        (raw_interface["day_rk"] == max_day_rk_df["day_rk"]) &
        (raw_interface["postcrm"] == max_day_rk_df["postcrm"]) &
        (raw_interface["precrm"] == max_day_rk_df["precrm"]),
        "left_semi"
    )
    .select(raw_interface["*"], correct_records["cis_code"].alias("updated_cis_code"))
)

# Display the updated raw_interface
updated_raw_interface.show()
