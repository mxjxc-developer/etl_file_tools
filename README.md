# etl_file_tools
![Coverage](https://codecov.io/gh/mxjxc-developer/etl_file_tools/branch/main/graph/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)  
*Repo to help with validating, cleaning, and integrating data from files to other systems.*

## 🚀 Features
- ✅ Fast and lightweight using pandas DataFrames
- ✅ Applies SQL constraint logic to dataframe to reduce bad data and increase data quality.
- ✅ Supports Python 3.11+
- ✅ Work in progress - more to come.

## 📦 Installation
```bash

# Clone the repository
git clone https://github.com/your-username/your-repo.git
cd your-repo

# Install dependencies using Poetry
poetry install
```
## 🛠️ Usage

```python
from etl_file_tools.file_load_dataframe import FileFrame

data = {
    'City': ['Milwaukee', 'Green Bay', 'Madison', 'Oshkosh', ],
    'Code': ['1', '2', '3', '4', ],
    'Population': [500, 300, 200, 100, ],
    'State_Id': [None, '00100', '00100', '00100', ]
}

# valid
custom_dataframe = FileFrame()
primary_key_constraint = custom_dataframe.constraint_primary_key(column_names=['City', 'Code'])
default_value_constraint = custom_dataframe.constraint_default_value(column_name='State_Id', default_value='00100')
custom_dataframe.add_constraint(constraint_func=primary_key_constraint)
custom_dataframe.add_constraint(constraint_func=default_value_constraint)
custom_dataframe.read_dict(data_dict=data)
print(custom_dataframe.dataframe)

# not valid - raises ValueError: Error:  NOT NULL Constraint:  State_Id cannot have null values.
custom_dataframe.remove_constraint(constraint_func=default_value_constraint)
not_null_constraint = custom_dataframe.constraint_not_null(column_name='State_Id')
custom_dataframe.add_constraint(constraint_func=not_null_constraint)
custom_dataframe.read_dict(data_dict=data)
```
## License
[MIT](https://choosealicense.com/licenses/mit/)
## 🧪 Running Tests
To run tests, use `pytest`:
```bash

poetry run pytest
# or
poetry run pytest --cov=etl_file_tools
# or
poetry run pytest --cov=etl_file_tools --cov-report=html
```