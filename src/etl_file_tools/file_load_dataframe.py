from __future__ import annotations
import pandas as pd
from typing import Literal
from dataclasses import dataclass, field

class FileFrame:
    """
    A class to load and manipulate DataFrames from a file.  The main purpose is to validate the data being loaded into the dataframe using
    constraint logic similar to SQL.

    This class provides functionality to load DataFrames from various file formats (Excel, CSV, Fixed Width) and apply constraints to the data to
    ensure data integrity. Constraints can be added using class methods, and they are validated before any modifications to the DataFrame.

    Attributes:
        _dataframe (pd.DataFrame): The internal DataFrame object that holds the data.
        _constraints (list): A list of constraint functions to validate the DataFrame data.
        _constraint_details (list): A list of constraint objects to validate the DataFrame data.
    """
    def __init__(self, *args, **kwargs) -> None:
        """
        Initializes the FileFrame instance with an empty DataFrame or data passed through *args and **kwargs.

        Args:
            *args: Positional arguments to be passed to pandas DataFrame constructor.
            **kwargs: Keyword arguments to be passed to pandas DataFrame constructor.
        """
        self._dataframe = pd.DataFrame(*args, **kwargs)
        self._constraints = []
        self._constraint_details = []

    @property
    def dataframe(self) -> pd.DataFrame:
        """
        This property allows access to the DataFrame that in the `FileFrame`.

        Returns:
            pd.DataFrame: The loaded DataFrame.
        """
        return self._dataframe

    @property
    def constraints(self) -> tuple:
        """
        This property allows access to the constraints that have been added to the instance of `FileFrame`. Constraints are functions that validate
        the data before any operation, such as appending or reading data.

        Returns:
            tuple: A tuple of constraint functions applied to the DataFrame.
        """
        return tuple(self._constraints)  # Using tuple to prevent modification

    @property
    def constraint_details(self) -> tuple:
        """
        This property allows access to the constraint details that have been added to the instance of `FileFrame`.  Each constraint in the constraint
        details tuple is an _Constraint instance which contains all the details/parameters for each constraint.

        Returns:
            tuple: A tuple of constraint details that have been added to the DataFrame.
        """
        return tuple(self._constraint_details)  # Using tuple to prevent modification

    @classmethod
    def constraint_not_null(cls, column_name: str) -> callable:
        """
        Creates a constraint function to check that a column does not contain any null values.

        Args:
            column_name (str): The name of the column to check for null values.

        Returns:
            callable: A function that checks if the specified column has any null values.

        Raises:
            ValueError: If the values in the specified column are null/missing.
        """
        def not_null_constraint(dataframe: pd.DataFrame) -> None:
            if dataframe[column_name].isnull().any():
                raise ValueError(f"Error:  NOT NULL Constraint:  {column_name} cannot have null values.")
        return not_null_constraint

    @classmethod
    def constraint_primary_key(cls, column_names: list) -> callable:
        """
        Creates a constraint function to ensure that values in the specified columns are unique and not null (primary key).

        Args:
            column_names (list): A list of column names to check for duplicate or null values.

        Returns:
            callable: A function that checks if there are any duplicates or nulls in the specified columns.

        Raises:
            ValueError: If the values in the specified columns are null/missing or if they contain duplicate values.
        """
        def primary_key_constraint(dataframe: pd.DataFrame) -> None:
            if dataframe[column_names].isnull().any(axis=1).any():
                raise ValueError(f"Error:  PRIMARY KEY Constraint:  {column_names} cannot have null values.")

            if any(dataframe.duplicated(subset=column_names)):
                raise ValueError(f"Error:  PRIMARY KEY Constraint:  {column_names} cannot have duplicate values.")
        return primary_key_constraint

    @classmethod
    def constraint_unique(cls, column_names: list) -> callable:
        """
        Creates a constraint function to ensure that values in the specified columns are unique (null values are allowed in unique constraint).

        Args:
            column_names (list): A list of column names to check for duplicate values.

        Returns:
            callable: A function that checks if there are any duplicates in the specified columns.

        Raises:
            ValueError: If the values in the specified columns contain duplicate values.
        """
        def unique_constraint(dataframe: pd.DataFrame) -> None:
            if any(dataframe.duplicated(subset=column_names)):
                raise ValueError(f"Error:  UNIQUE Constraint:  {column_names} cannot have duplicate values.")
        return unique_constraint

    @classmethod
    def constraint_check(cls, column_name: str, check_condition: Literal['=', '>', '<', '>=', '<=', '!='], check_value: int | str | float) -> callable:
        """
        Creates a constraint function to check that the values in a specified column satisfy a condition.

        Args:
            column_name (str): The column name to check the condition on.
            check_condition (Literal): The condition to check ('=', '>', '<', '>=', '<=', '!=').
            check_value (int | str | float): The value to compare the column values against.

        Returns:
            callable: A function that checks the condition for the specified column.

        Raises:
            ValueError: If the condition is not recognized or if the values in the column don't meet the condition.
        """
        def check_constraint(dataframe: pd.DataFrame) -> None:
            if check_condition.upper().strip() == '=':
                if not (dataframe[column_name] == check_value).all():
                    raise ValueError(f"Error:  CHECK Constraint:  All values in {column_name} must be equal to {check_value}.")
            elif check_condition.upper().strip() == '>':
                if not (dataframe[column_name] > check_value).all():
                    raise ValueError(f"Error:  CHECK Constraint:  All values in {column_name} must be greater than {check_value}.")
            elif check_condition.upper().strip() == '<':
                if not (dataframe[column_name] < check_value).all():
                    raise ValueError(f"Error:  CHECK Constraint:  All values in {column_name} must be less than {check_value}.")
            elif check_condition.upper().strip() == '>=':
                if not (dataframe[column_name] >= check_value).all():
                    raise ValueError(f"Error:  CHECK Constraint:  All values in {column_name} must be greater than or equal to {check_value}.")
            elif check_condition.upper().strip() == '<=':
                if not (dataframe[column_name] <= check_value).all():
                    raise ValueError(f"Error:  CHECK Constraint:  All values in {column_name} must be less than or equal to {check_value}.")
            elif check_condition.upper().strip() == '!=':
                if not (dataframe[column_name] != check_value).all():
                    raise ValueError(f"Error:  CHECK Constraint:  All values in {column_name} must not equal {check_value}.")
            else:
                raise ValueError(f"Error:  Unsupported equality condition: {check_condition}")
        return check_constraint

    @classmethod
    def constraint_default_value(cls, column_name: str, default_value: int | str | float, **kwargs) -> callable:
        """
        Creates a constraint function to default null/missing values in a column to a value.

        Args:
            column_name (str): The name of the column to default null/missing values.
            default_value (int | str | float): The value to default to if the column value is null.
            **kwargs: Additional keyword arguments to pass to `pd.DataFrame.fillna()`.

        Returns:
            callable: A function that defaults null/missing values to a value.
        """
        def default_value_constraint(dataframe: pd.DataFrame) -> None:
            dataframe[column_name] = dataframe[column_name].fillna(value=default_value, **kwargs)
        return default_value_constraint

    def add_constraint(self, constraint_func: callable) -> None:
        """
        Adds a constraint function to the list of constraints.

        Args:
            constraint_func (callable): The constraint function to be added.

        Raises:
            ValueError: If the same constraint is applied to the same columns or if more than one primary key constraint exists.
        """
        input_constraint = FileFrame._get_constraint_closure_function_details(func=constraint_func)
        for constraint in self.constraint_details:
            if (input_constraint.constraint_name == 'primary_key_constraint'
                    and input_constraint.constraint_name == constraint.constraint_name):
                raise ValueError(f"Error:  There can only be one primary key constraint on a dataframe.")

            if (input_constraint.constraint_name == 'not_null_constraint' and constraint.constraint_name == 'primary_key_constraint'
                    and input_constraint.column_name in constraint.column_names):
                raise ValueError(f"Error:  There is already a primary key not null constraint on the column {input_constraint.column_name}.")

            if input_constraint == constraint:
                column_variable = input_constraint.column_names if input_constraint.column_name is None else input_constraint.column_name
                raise ValueError(f"Error:  The {constraint.constraint_name} already exists on column(s) - {column_variable}.")

        self._constraints.append(constraint_func)
        self._constraint_details.append(input_constraint)

    def remove_constraint(self, constraint_func: callable) -> None:
        """
        If the constraint func exists, this function removes the constraint function from the list as well as the constraint object from the list
        of constraint details.

        Args:
            constraint_func (callable): The constraint function to be removed.
        """
        input_constraint = FileFrame._get_constraint_closure_function_details(func=constraint_func)
        self._constraints = [func for func in self.constraints if func != constraint_func]
        self._constraint_details = [details for details in self.constraint_details if details != input_constraint]

    def find_duplicate_records(self, column_names: list, **kwargs) -> pd.DataFrame:

        """
        Finds and returns rows in the DataFrame that have duplicate values based on the specified columns.  Additional options for the
        `duplicated()` function can be provided through `kwargs`.

        Args:
            column_names (list): A list of column names to check for duplicates.
            **kwargs: Additional keyword arguments to pass to the `duplicated()` method. These may include: - `keep`: Determines which duplicates to
                      mark as `True`. Options are 'first', 'last', or `False` (all duplicates).

        Returns:
            pd.DataFrame: A DataFrame containing only the rows that are duplicates based on the specified columns.
        """
        duplicate_dataframe = self.dataframe[column_names].duplicated(**kwargs)
        return self.dataframe[duplicate_dataframe]

    def read_excel(self, file_path: str, sheet_name: int=0, **kwargs) -> None:
        """
        Reads data from an Excel file and loads it into the DataFrame after validating constraints.  Additional options for the `read_excel()`
        function can be provided through `kwargs`.

        Args:
            file_path (str): The path to the Excel file.
            sheet_name (int, optional): The sheet index or name to read. Defaults to 0.
            **kwargs: Additional keyword arguments to pass to `pd.read_excel()`.

        Raises:
            ValueError: If any of the constraints are violated after reading the data.
        """
        self._dataframe = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
        self._validate_constraints()

    def read_csv(self, file_path: str, sep: str = ',', **kwargs) -> None:
        """
        Reads data from a CSV file and loads it into the DataFrame after validating constraints.  Additional options for the `read_csv()` function
        can be provided through `kwargs`.

        Args:
            file_path (str): The path to the CSV file.
            sep (str, optional): The delimiter for separating values in the CSV file. Defaults to ','.
            **kwargs: Additional keyword arguments to pass to `pd.read_csv()`.

        Raises:
            ValueError: If any of the constraints are violated after reading the data.
        """
        self._dataframe = pd.read_csv(file_path, sep=sep, **kwargs)
        self._validate_constraints()

    def read_fwf(self, file_path: str, colspecs: list) -> None:
        """
        Reads data from a fixed-width file and loads it into the DataFrame after validating constraints.  Additional options for the `read_fwf()`
        function can be provided through `kwargs`.

        Args:
            file_path (str): The path to the fixed-width file.
            colspecs (list): A list of column specifications (start and end positions for columns).

        Raises:
            ValueError: If any of the constraints are violated after reading the data.
        """
        self._dataframe = pd.read_fwf(file_path, colspecs=colspecs)
        self._validate_constraints()

    def read_dict(self, data_dict: dict, **kwargs) -> None:
        """
        Reads data from a dictionary and loads it into the DataFrame after validating constraints.  Additional options for the `read_dict()` function
        can be provided through `kwargs`.

        Args:
            data_dict (str): The dictionary to load into the dataframe.
            **kwargs: Additional keyword arguments to pass to `read_dict`.

        Raises:
            ValueError: If any of the constraints are violated after reading the data.
        """
        self._dataframe = pd.DataFrame(data_dict, **kwargs)
        self._validate_constraints()

    def column_names(self) -> list:
        """
        Returns a list of the DataFrame column names.

        Returns:
            list: A list of the column names of the DataFrame.
        """
        return self.dataframe.columns.tolist()

    def column_datatypes(self) -> dict:
        """
        Returns a dict of the DataFrame column datatypes - key: column_name, value: column_datatype.

        Returns:
            dict: A dict of the column datatypes of the DataFrame where the key is the column name and value is the
            column datatype.
        """
        datatypes =  self.dataframe.dtypes
        return datatypes.to_dict()

    def _validate_constraints(self) -> None:
        """
        Validates the DataFrame against all added constraints.

        Raises:
            ValueError: If any constraint is violated.
        """
        for constraint in self._constraints:
            constraint(self.dataframe)

    @staticmethod
    def _get_constraint_closure_function_details(func: callable) -> _Constraint:
        """
        Returns a _Constraint object that consists of the closure function parameters and values in a dict, name , value.

        Returns:
            _Constraint: A constraint object that contains all the details for each constraint inputted.
        """
        func_details = {}
        for i, cell in enumerate(func.__closure__):
            param_name = func.__code__.co_freevars[i]
            param_value = cell.cell_contents
            if param_name != 'kwargs':
                func_details['constraint_name'] = func.__name__
                func_details[param_name] = param_value
        return _Constraint(**func_details)

    def __repr__(self) -> repr:
        """
        Returns a string representation of the FileFrame object.

        Returns:
            str: A string representation of the internal DataFrame.
        """
        return repr(self.dataframe)

    def __str__(self) -> str:
        """
        Returns a string of the FileFrame object.

        Returns:
            str: A string of the internal DataFrame.
        """
        return f"{self.__class__.__name__}"

@dataclass(frozen=True)
class _Constraint:
    """Represents a database constraint with various attributes.  This class is an internal class used only for maintaining data for each constraint.
    Think of it only as a container that allows for easy access of the constraint parameters.

    Attributes:
        constraint_name (str): The name of the constraint.
        column_names (list): A list of column names affected by the constraint.
            Defaults to an empty list.
        column_name (str, optional): A single column name affected by the constraint.
            Defaults to None.
        check_condition (str, optional): The condition for a check constraint.
            Defaults to None.
        check_value (int | str | float, optional): The value to check against in a check constraint.
            Defaults to None.
        default_value (int | str | float, optional): The value to default to in the default value constraint.
            Defaults to None.
    """
    constraint_name: str
    column_names: list = field(default_factory=list)
    column_name: str = None
    check_condition: str = None
    check_value: int | str | float = None
    default_value: int | str | float = None