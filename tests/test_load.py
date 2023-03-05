from etl.load import send_to_csv
import pandas as pd
import os
import tempfile


def test_send_to_csv():
    with tempfile.TemporaryDirectory() as temp_dir:
        # Set up
        data = {'col1': ['a', 'b', 'c'], 'col2': [1, 2, 3]}
        df = pd.DataFrame(data)

        # Call
        filename = 'test.csv'
        send_to_csv(df, filename, temp_dir)

        # Verify
        filepath = os.path.join(temp_dir, filename)
        assert os.path.exists(filepath)
        expected_content = 'col1,col2\na,1\nb,2\nc,3\n'
        with open(filepath, 'r') as f:
            assert f.read() == expected_content
