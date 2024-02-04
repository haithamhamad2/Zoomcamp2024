import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Preprocessing: rows with zero passengers:", data['passenger_count'].isin([0]).sum() )
    #data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # data.columns = (data.columns
    #                 .str.replace(' ','_')
    #                 .str.lower())
    #camel_case_columns = [col for col in data.columns if re.match(r'^[a-z]+(?:[A-Z][a-z]*)*$', col)]
    #print(camel_case_columns)

    # Regular expression for camel case
    camel_case_pattern = re.compile(r'^[A-Z][a-zA-Z0-9]*$')
    # Count the number of columns in camel case
    camel_case_columns = sum(camel_case_pattern.match(column) is not None for column in data.columns)
    print(f'The DataFrame has {camel_case_columns} columns in camel case.')

    # regular expression for camel case starting with uppercase
    regex = r'^[A-Z][a-zA-Z0-9]*$'

    # filter columns
    filtered_columns = [col for col in data.columns if re.match(regex, col)]
    #print(filtered_columns)
    #print(len(filtered_columns))
    #convert camel case to snake case
    data.columns = data.columns.map(lambda x: re.sub(r'(?<=[a-z])(?=[A-Z])', '_', x).lower())
    
    num_unique_values = data['vendor_id'].nunique()

    print(f'There are {num_unique_values} unique values in the column.')

    unique_values = data['vendor_id'].unique()
    print(f'The unique values in the column are: {unique_values}')
    
    
    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert 'vendor_id' in output.columns, 'there is no vendor id column'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 passenger'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are no 0 trip distance records'
    