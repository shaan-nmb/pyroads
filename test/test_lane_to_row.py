




def test_lane_to_row():
    import pandas as pd
    import numpy as np
    from pyroads.mappings.cross_sectional_position import lane_to_row
    
    input_data = pd.DataFrame(
        columns=[
            "road_no",
            "carriageway",
            "dirn",
            "slk_from", "slk_to",
            "true_from", "true_to",
            "A_1", "A_2", "B_1"],
        data=[
            ["H001", "L", "L", 0.010, 0.050, 0.010, 0.050, "a", "0", 9],
            ["H001", "L", "L", 0.050, 0.080, 0.050, 0.080, "b", "1", 8],
            ["H001", "L", "L", 0.080, 0.100, 0.080, 0.100, "c", "2", 7],
            ["H001", "L", "L", 0.100, 0.120, 0.100, 0.120, "d", "3", 6],
        ]
    )
    
    result = lane_to_row(
        data       = input_data,
        dirn       = "dirn",
        idvars     = ["road_no","carriageway"],
        start      = "slk_from",
        end        = "slk_to",
        start_true = "true_from",
        end_true   = "true_to",
        prefixes   = ["A_", "B_"]
    )

    expected_result = pd.DataFrame(
        columns=[
            "road_no",
            "carriageway",
            "slk_from",   "slk_to",
            "true_from", "true_to",
            "XSP",
            "A_", "B_",
        ],
        data=[
            ["H001","L", 0.01, 0.05, 0.01, 0.05, "L1", "a", 9.0    ],
            ["H001","L", 0.05, 0.08, 0.05, 0.08, "L1", "b", 8.0    ],
            ["H001","L", 0.08, 0.10, 0.08, 0.10, "L1", "c", 7.0    ],
            ["H001","L", 0.10, 0.12, 0.10, 0.12, "L1", "d", 6.0    ],
            ["H001","L", 0.01, 0.05, 0.01, 0.05, "L2", "0", np.nan ],
            ["H001","L", 0.05, 0.08, 0.05, 0.08, "L2", "1", np.nan ],
            ["H001","L", 0.08, 0.10, 0.08, 0.10, "L2", "2", np.nan ],
            ["H001","L", 0.10, 0.12, 0.10, 0.12, "L2", "3", np.nan ],
        ]
    )

    pd.testing.assert_frame_equal(
        result,
        expected_result,
        check_like=True, # ignore column / row order
        check_dtype=False, # ignore dtype
    )