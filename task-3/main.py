from flask import Flask, jsonify
import pandas as pd
import numpy as np
import time


app = Flask(__name__)

def calculate_series_a_funding():

    total_raised_amt = 0
    total_row_count = 0
    average_raised_amt = 0

    chunksize = 1000
    # start = time.monotonic()
    # start_time = time.time()
    for chunk in pd.read_csv('python_task_data.csv', usecols=['round', 'raisedAmt'], chunksize=chunksize):
        chunk['raisedAmt'] = chunk['raisedAmt'].astype('uint32')
        # ideal for repated repetitive data, and small set of distinct values...
        chunk['round'] = chunk['round'].astype('category')    
        chunk_series_a = chunk[chunk['round'] == 'a']
        chunk_series_a.dropna()

        total_raised_amt += chunk_series_a['raisedAmt'].sum()
        total_row_count += chunk_series_a['raisedAmt'].count()

    # end_time = time.time()
    # end = time.monotonic()
    average_raised_amt = total_raised_amt / total_row_count

    # execution_time1 = end_time - start_time
    # execution_time2 = end - start

    return total_raised_amt, average_raised_amt

@app.route('/', methods=['GET'])
def series_a_funding():
    total_raised_amt, average_raised_amt = calculate_series_a_funding()

    return jsonify({
        'total': f"${total_raised_amt:,.2f}",
        'average': f"${average_raised_amt:,.2f}",
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
