import re
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

@transformer
def transform(data, *args, **kwargs):

    # Specify your transformation logic here
    print('sum of empty passenger count:', data['passenger_count'].isin([0]).sum())
    print('sum of empty trip distance:', data['trip_distance'].isin([0]).sum())
    print('Maximum number in VendorID:', data['VendorID'].isin([2]).sum())
    

    data.columns = (
        data.columns
        .str.replace(' ', '_')
        
    )
    #convert column names to snakecase
    data.columns = (camel_to_snake(col) for col in data.columns)

    print('Finding sum of new column vendor_id in columns:', data['vendor_id'].sum())

    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output ['passenger_count'].isin([0]).sum() == 0
    assert output ['trip_distance'].isin([0]).sum() == 0
    assert output ['vendor_id'].sum()

