
def test_interval_merge():
    """
    Test for proposed interface change to interval_merge.
    This is for discussion purposes.
    Currently this test fails as this approach is not implemented
    """
    import pyroads.reshape.merge as merge

    import pandas as pd

    segments = pd.DataFrame(
        columns=["road", "slk_from", "slk_to"],
        data=[
            ["H001",   0, 100],
            ["H001", 100, 200],
            ["H001", 200, 300],
            ["H001", 300, 400],
        ]
    )

    data = pd.DataFrame(
        columns=["road", "slk_from", "slk_to", "measure", "category"],
        data=[                             # overlaps lengths
            ["H001", 50,  140, 1.0, "A"],  # 50  40   0  0
            ["H001", 140, 160, 2.0, "B"],  #  0  20   0  0
            ["H001", 160, 180, 3.0, "B"],  #  0  20   0  0
            ["H001", 180, 220, 4.0, "B"],  #  0  20  20  0
            ["H001", 220, 240, 5.0, "C"],  #  0   0  20  0
            ["H001", 240, 260, 5.0, "C"],  #  0   0  20  0
            ["H001", 260, 280, 6.0, "D"],  #  0   0  20  0
            ["H001", 280, 300, 7.0, "E"],  #  0   0  20  0
            ["H001", 300, 320, 8.0, "F"],  #  0   0     20
        ]
    )

    expected_result = pd.DataFrame(
        columns=["road", "slk_from", "slk_to",  "measure longest value",  "category longest value"],
        data=[
            ["H001",   0, 100,  1.0,  "A"],
            ["H001", 100, 200,  1.0,  "B"],
            ["H001", 200, 300,  5.0,  "C"],
            ["H001", 300, 400,  8.0,  "F"],
        ]
    )

    result = merge.on_intervals(
        left_df=segments,
        right_df=data,
        idvars=["road"],
        start="slk_from",
        end="slk_to",
        column_actions=[
            merge.Action('measure',  rename="measure longest value",  aggregation=merge.MODE),
            merge.Action('measure',  rename="measure longest value",  aggregation=merge.SUM ),
            merge.Action('category', rename="category longest value", aggregation=merge.MODE),
        ]
    )

    pd.testing.assert_frame_equal(result, expected_output)