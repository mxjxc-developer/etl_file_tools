import pytest
from unittest.mock import patch
from etl_file_tools.file_load_dataframe import FileFrame
import pandas as pd
import re

data = {
    'Id': ['001', '001', '003', None, ],
    'Name': ['Alice', None, 'Charlie', 'David', ],
    'Age': [25, 30, 35, 40, ],
    'City': ['Milwaukee', 'Los Angeles', 'Chicago', 'Houston', ],
    'Code': ['1', '1', '2', '3', ],
    'Test': [1, 2, 3, 4, ],
    'Test2': [1, 1, 1, 1, ],
    'Test3': [1, 1, 3, 4, ],
}

@pytest.fixture()
def custom_dataframe():
    dataframe = FileFrame()
    return dataframe

@pytest.fixture
def mock_dataframe():
    """Fixture to return a mock DataFrame."""
    return pd.DataFrame({'A': [None, 2, 3], 'B': [4, 5, 6]})

def test_get_dataframe(custom_dataframe):
    custom_dataframe.read_dict(data_dict=data)
    dataframe = custom_dataframe.dataframe
    assert isinstance(dataframe, pd.DataFrame)

def test_get_constraints(custom_dataframe):
    custom_dataframe.read_dict(data_dict=data)
    constraints = custom_dataframe.constraints
    assert isinstance(constraints, tuple)

def test_get_constraint_details(custom_dataframe):
    custom_dataframe.read_dict(data_dict=data)
    constraint_details = custom_dataframe.constraint_details
    assert isinstance(constraint_details, tuple)

def test_constraint_not_null(custom_dataframe):
    # initialize the not null constraint on the city column
    not_null_constraint_city = custom_dataframe.constraint_not_null(column_name='City')
    # add the not_null_constraint_city to the dataframe
    custom_dataframe.add_constraint(constraint_func=not_null_constraint_city)
    # build the dataframe - since the City column has no nulls there should be exception raised
    custom_dataframe.read_dict(data_dict=data)

def test_constraint_not_null_error(custom_dataframe):
    # initialize the not null constraint on the id column
    not_null_constraint_id = custom_dataframe.constraint_not_null(column_name='Id')
    # add the not_null_constraint_id to the dataframe
    custom_dataframe.add_constraint(constraint_func=not_null_constraint_id)
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  NOT NULL Constraint:  Id cannot have null values.")):
        custom_dataframe.read_dict(data_dict=data)

def test_constraint_primary_key(custom_dataframe):
    # initialize the primary key constraint on the city column
    primary_key_constraint_city = custom_dataframe.constraint_primary_key(column_names=['City'])
    # add the primary_key_constraint_city to the dataframe
    custom_dataframe.add_constraint(constraint_func=primary_key_constraint_city)
    # build the dataframe - since the City column has no nulls or no duplicates there should be exception raised
    custom_dataframe.read_dict(data_dict=data)

def test_constraint_primary_key_duplicate_error(custom_dataframe):
    # initialize the primary key constraint on the code column
    primary_key_constraint_code = custom_dataframe.constraint_primary_key(column_names=['Code'])
    # add the primary_key_constraint_code to the dataframe
    custom_dataframe.add_constraint(constraint_func=primary_key_constraint_code)
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  PRIMARY KEY Constraint:  ['Code'] cannot have duplicate values.")):
        custom_dataframe.read_dict(data_dict=data)

def test_constraint_primary_key_null_error(custom_dataframe):
    # initialize the primary key constraint on the id column
    primary_key_constraint_id = custom_dataframe.constraint_primary_key(column_names=['Id'])
    # add the primary_key_constraint_id to the dataframe
    custom_dataframe.add_constraint(constraint_func=primary_key_constraint_id)
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  PRIMARY KEY Constraint:  ['Id'] cannot have null values.")):
        custom_dataframe.read_dict(data_dict=data)

def test_constraint_unique(custom_dataframe):
    # initialize the unique constraint on the age column
    unique_constraint_age = custom_dataframe.constraint_unique(column_names=['Age'])
    # add the unique_constraint_age to the dataframe
    custom_dataframe.add_constraint(constraint_func=unique_constraint_age)
    # build the dataframe - since the age column no duplicates there should be exception raised
    custom_dataframe.read_dict(data_dict=data)

def test_constraint_unique_error(custom_dataframe):
    # initialize the unique constraint on the id column
    unique_constraint_id = custom_dataframe.constraint_unique(column_names=['Id'])
    # add the unique_constraint_id to the dataframe
    custom_dataframe.add_constraint(constraint_func=unique_constraint_id)
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  UNIQUE Constraint:  ['Id'] cannot have duplicate values.")):
        custom_dataframe.read_dict(data_dict=data)

def test_constraint_check(custom_dataframe):
    # initialize the unique constraint on the id column
    check_constraint_age = custom_dataframe.constraint_check(column_name='Age', check_condition='>', check_value=10)
    # add the check_constraint_age to the dataframe
    custom_dataframe.add_constraint(constraint_func=check_constraint_age)
    # build the dataframe - since the age column values are all greater than 10 there should be no exception raised
    custom_dataframe.read_dict(data_dict=data)

@pytest.mark.parametrize(
    'column_name, condition, value, expected_result', [
        ('Age', '=', 10, "Error:  CHECK Constraint:  All values in Age must be equal to 10."),
        ('Age', '>', 100, "Error:  CHECK Constraint:  All values in Age must be greater than 100."),
        ('Age', '<', 1, "Error:  CHECK Constraint:  All values in Age must be less than 1."),
        ('Age', '>=', 100, "Error:  CHECK Constraint:  All values in Age must be greater than or equal to 100."),
        ('Age', '<=', 1, "Error:  CHECK Constraint:  All values in Age must be less than or equal to 1."),
        ('Age', '!=', 30, "Error:  CHECK Constraint:  All values in Age must not equal 30."),
        ('Age', 'in', [500, 600], "Error:  CHECK Constraint:  All values in Age must be in [500, 600]."),
        ('Age', 'bad', 0, "Error:  Unsupported check condition: bad"),
    ],
)
def test_constraint_check_error(custom_dataframe, column_name, condition, value, expected_result):
    # initialize the check constraint on the age column
    check_constraint_age = custom_dataframe.constraint_check(column_name=column_name, check_condition=condition, check_value=value)
    # add the check_constraint_age to the dataframe
    custom_dataframe.add_constraint(constraint_func=check_constraint_age)
    with pytest.raises(expected_exception=ValueError, match=re.escape(expected_result)):
        custom_dataframe.read_dict(data_dict=data)

def test_constraint_default_value(custom_dataframe):
    # initialize the default value constraint on the id column
    default_value_constraint_id = custom_dataframe.constraint_default_value(column_name='Id', default_value='004')
    # add the default_value_constraint_id to the dataframe
    custom_dataframe.add_constraint(constraint_func=default_value_constraint_id)
    # build the dataframe
    custom_dataframe.read_dict(data_dict=data)
    # confirm no more null values exist in the id column
    assert custom_dataframe.dataframe['Id'].isnull().sum() == 0

def test_add_constraint_duplicate_not_null_constraint_error(custom_dataframe):
    # initialize the two not null constraint on same column id
    not_null_constraint_id_1 = custom_dataframe.constraint_not_null(column_name='Id')
    not_null_constraint_id_2 = custom_dataframe.constraint_not_null(column_name='Id')
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  The not_null_constraint already exists on column(s) - Id.")):
        #  add the not_null_constraint_id_1 to the dataframe
        custom_dataframe.add_constraint(constraint_func=not_null_constraint_id_1)
        #  add the not_null_constraint_id_2 to the dataframe
        custom_dataframe.add_constraint(constraint_func=not_null_constraint_id_2)
        # since a not null constraint already exists on the id column an exception should be raised

def test_add_constraint_duplicate_primary_key_constraint_error(custom_dataframe):
    # initialize the primary key constraint on same columns city and test
    primary_key_constraint_1 = custom_dataframe.constraint_primary_key(column_names=['City', 'Test'])
    primary_key_constraint_2 = custom_dataframe.constraint_primary_key(column_names=['City', 'Test'])
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  There can only be one primary key constraint on a dataframe.")):
        #  add the primary_key_constraint_1 to the dataframe
        custom_dataframe.add_constraint(constraint_func=primary_key_constraint_1)
        #  add the primary_key_constraint_2 to the dataframe
        custom_dataframe.add_constraint(constraint_func=primary_key_constraint_2)
        # since a primary key constraint already exists on city and test column an exception should be raised

def test_add_constraint_not_null_constraint_on_primary_key_constraint_error(custom_dataframe):
    # initialize the primary key constraint on columns city and test and then a not null constraint on city
    primary_key_constraint = custom_dataframe.constraint_primary_key(column_names=['City', 'Test'])
    not_null_constraint = custom_dataframe.constraint_not_null(column_name='City')
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  There is already a primary key not null constraint on the column City.")):
        #  add the primary_key_constraint to the dataframe
        custom_dataframe.add_constraint(constraint_func=primary_key_constraint)
        #  add the not null to the dataframe
        custom_dataframe.add_constraint(constraint_func=not_null_constraint)
        # since a primary key not null constraint already exists on city column an exception should be raised when trying to apply the not null constraint on column city

def test_add_constraint_duplicate_unique_constraint_error(custom_dataframe):
    # initialize the unique constraint on same columns city and code
    unique_constraint_1 = custom_dataframe.constraint_unique(column_names=['City', 'Code'])
    unique_constraint_2 = custom_dataframe.constraint_unique(column_names=['City', 'Code'])
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  The unique_constraint already exists on column(s) - ['City', 'Code'].")):
        #  add the unique_constraint_1 to the dataframe
        custom_dataframe.add_constraint(constraint_func=unique_constraint_1)
        #  add the unique_constraint_2 to the dataframe
        custom_dataframe.add_constraint(constraint_func=unique_constraint_2)
        # since a unique constraint already exists on the city and code columns an exception should be raised

def test_add_constraint_duplicate_check_constraint_error(custom_dataframe):
    # initialize the check constraint on same column age
    check_constraint_1 = custom_dataframe.constraint_check(column_name='Age', check_condition='>', check_value=0)
    check_constraint_2 = custom_dataframe.constraint_check(column_name='Age', check_condition='>', check_value=0)
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  The check_constraint already exists on column(s) - Age.")):
        #  add the check_constraint_1 to the dataframe
        custom_dataframe.add_constraint(constraint_func=check_constraint_1)
        #  add the check_constraint_2 to the dataframe
        custom_dataframe.add_constraint(constraint_func=check_constraint_2)
        # since a check constraint already exists on the age column an exception should be raised

def test_add_constraint_duplicate_default_value_constraint_error(custom_dataframe):
    # initialize the default value constraint on same column id
    default_value_constraint_1 = custom_dataframe.constraint_default_value(column_name='Id', default_value='004')
    default_value_constraint_2 = custom_dataframe.constraint_default_value(column_name='Id', default_value='004')
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  The default_value_constraint already exists on column(s) - Id.")):
        #  add the default_value_constraint_1 to the dataframe
        custom_dataframe.add_constraint(constraint_func=default_value_constraint_1)
        #  add the default_value_constraint_2 to the dataframe
        custom_dataframe.add_constraint(constraint_func=default_value_constraint_2)
        # since a default value constraint already exists on the id column an exception should be raised

def test_remove_constraint(custom_dataframe):
    primary_key_constraint = custom_dataframe.constraint_primary_key(column_names=['City', 'Test'])
    check_constraint_age = custom_dataframe.constraint_check(column_name='Age', check_condition='>', check_value=10)
    unique_constraint_id = custom_dataframe.constraint_unique(column_names=['Id'])
    not_null_constraint_id = custom_dataframe.constraint_not_null(column_name='Id')
    custom_dataframe.add_constraint(constraint_func=primary_key_constraint)
    custom_dataframe.add_constraint(constraint_func=check_constraint_age)
    custom_dataframe.add_constraint(constraint_func=unique_constraint_id)
    custom_dataframe.add_constraint(constraint_func=not_null_constraint_id)
    custom_dataframe.remove_constraint(constraint_func=primary_key_constraint)
    custom_dataframe.remove_constraint(constraint_func=check_constraint_age)
    custom_dataframe.remove_constraint(constraint_func=unique_constraint_id)
    custom_dataframe.remove_constraint(constraint_func=not_null_constraint_id)
    assert len(custom_dataframe.constraints) == 0 and len(custom_dataframe.constraint_details) == 0

@pytest.mark.parametrize(
    'keep, expected_result', [
        (False, 2),
        ('first', 1),
        ('last', 1),
    ],
)
def test_find_duplicate_records(custom_dataframe, keep, expected_result):
    # build the dataframe
    custom_dataframe.read_dict(data_dict=data)
    # find the duplicate values in columns test2, test3
    duplicates = custom_dataframe.find_duplicate_records(column_names=['Test2', 'Test3'], keep=keep)
    # check records returned in dataframe
    assert duplicates.shape[0] == expected_result

def test_read_excel(mock_dataframe):
    # initialize the custom dataframe
    custom_dataframe = FileFrame()
    # initialize the constraint to test the validate constraints method call
    constraint_1 = custom_dataframe.constraint_not_null(column_name='A')
    # add the constraint to the dataframe
    custom_dataframe.add_constraint(constraint_func=constraint_1)
    # mock pandas.read_excel
    with patch("pandas.read_excel", return_value=mock_dataframe) as mock_read_excel:
        with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  NOT NULL Constraint:  A cannot have null values.")):
            # test that a not null constraint is raised to test the validate constraint method call
            custom_dataframe.read_excel(file_path="fake_path.xlsx")
            # test pandas.read_excel is called
            mock_read_excel.assert_called_once_with("fake_path.xlsx", sheet_name=0)
            # test dataframe equality
            pd.testing.assert_frame_equal(custom_dataframe.dataframe, mock_dataframe)

def test_read_csv(mock_dataframe):
    # initialize the custom dataframe
    custom_dataframe = FileFrame()
    # initialize the constraint to test the validate constraints method call
    constraint_1 = custom_dataframe.constraint_not_null(column_name='A')
    # add the constraint to the dataframe
    custom_dataframe.add_constraint(constraint_func=constraint_1)
    # mock pandas.read_csv
    with patch("pandas.read_csv", return_value=mock_dataframe) as mock_read_csv:
        with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  NOT NULL Constraint:  A cannot have null values.")):
            # test that a not null constraint is raised to test the validate constraint method call
            custom_dataframe.read_csv(file_path="fake_path.csv", sep=",")
            # test pandas.read_csv is called
            mock_read_csv.assert_called_once_with("fake_path.csv", sep=",")
            # test dataframe equality
            pd.testing.assert_frame_equal(custom_dataframe.dataframe, mock_dataframe)

def test_read_fwf(mock_dataframe):
    # initialize the custom dataframe
    custom_dataframe = FileFrame()
    # initialize the constraint to test the validate constraints method call
    constraint_1 = custom_dataframe.constraint_not_null(column_name='A')
    # add the constraint to the dataframe
    custom_dataframe.add_constraint(constraint_func=constraint_1)
    # mock pandas.read_fwf
    with patch("pandas.read_fwf", return_value=mock_dataframe) as mock_read_fwf:
        with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  NOT NULL Constraint:  A cannot have null values.")):
            # test that a not null constraint is raised to test the validate constraint method call
            custom_dataframe.read_fwf(file_path="fake_path.txt", colspecs=[(0, 5), (6, 10)])
            # test pandas.read_fwf is called
            mock_read_fwf.assert_called_once_with("fake_path.txt", colspecs=[(0, 5), (6, 10)])
            # test dataframe equality
            pd.testing.assert_frame_equal(custom_dataframe.dataframe, mock_dataframe)

def test_read_dict(custom_dataframe):
    # initialize the constraint to test the validate constraints method call
    constraint1 = custom_dataframe.constraint_not_null(column_name='Id')
    # add the constraint to the dataframe
    custom_dataframe.add_constraint(constraint_func=constraint1)
    with pytest.raises(expected_exception=ValueError, match=re.escape("Error:  NOT NULL Constraint:  Id cannot have null values.")):
        # test that a not null constraint is raised to test the validate constraint method call
        custom_dataframe.read_dict(data_dict=data)
        # test dataframe equality
        pd.testing.assert_frame_equal(custom_dataframe.dataframe, pd.DataFrame(data))

def test_column_names(custom_dataframe):
    custom_dataframe.read_dict(data_dict=data)
    column_names = custom_dataframe.column_names()
    assert isinstance(column_names, list)

def test_column_datatypes(custom_dataframe):
    custom_dataframe.read_dict(data_dict=data)
    column_datatypes = custom_dataframe.column_datatypes()
    assert isinstance(column_datatypes, dict)