import duckdb
from flask import Flask, render_template, request
import pandas as pd
import base64
import os

app = Flask(__name__)

# Create a DuckDB in-memory database
con = duckdb.connect(database=':memory:')

# Create a table to store the file data if it doesn't exist
con.execute("CREATE TABLE IF NOT EXISTS files (name TEXT, data BLOB)")

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Get the uploaded CSV file
        file = request.files['csv_file']
        
        filename = file.filename
        print("filename: ", filename)

        file_path = os.path.join(app.root_path, filename)
        print("file_path", file_path)
        file.save(file_path)

        con.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{file_path}', header=true)")
       
        query = request.form['query']

        result = con.execute(query).fetchall()
        
        # Get the column names
        columns = [description[0] for description in con.cursor().description()]
        
        return render_template('index.html', columns=columns, rows=result)
    
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
