import pytest
from main import EventScraper


# this should validate all versions from https://web3py.readthedocs.io/en/stable/web3.eth.html#filters
@pytest.mark.parametrize(
    "event_name,argument_filters,expected_outcome",
    [
        (
            "PairCreated",
            {"fromBlock": "latest"},
            'contract.events.PairCreated.createFilter(fromBlock="latest")',
        ),
        ("X", {"Y": "Z"}, 'contract.events.X.createFilter(Y="Z")'),
        (
            "PairCreated",
            {
                "fromBlock": "latest",
                "toBlock": "pending"},
            'contract.events.PairCreated.createFilter(fromBlock="latest", toBlock="pending")',
        ),
        (
                "PairCreated",
                {
                    "fromBlock": 100,
                    "toBlock": 200},
                'contract.events.PairCreated.createFilter(fromBlock=100, toBlock=200)',
        ),
    ],
)
def test_string_creation_function(event_name, argument_filters, expected_outcome):
    outcome = EventScraper.compose_filter_creation_execution_string(
        event_name, argument_filters
    )
    assert outcome == expected_outcome
