import os
import pathlib
import pandas

"""
You are welcome to use this structure as a starting point, or you can start from scratch.

The prefixed `get_` methods should not need any adjustment to read in the data.
Your solutions should be mostly contained within the prefixed `generate_` methods, and the `data_investigation()`
"""

# --- Fill in your details here ---
FIRST_NAME = "Simon"
LAST_NAME = "Lan"

# Gets current path
CURRENT_DIR = pathlib.Path(__file__).parent
DATA_DIR = os.path.join(CURRENT_DIR, "data")


def get_exchange_data():
    exchange_data = pandas.read_csv(
        os.path.join(DATA_DIR, "exchange.data"),
        delimiter="|",
    )
    return exchange_data


def get_stock_list():
    stock_list = pandas.read_csv(os.path.join(DATA_DIR, "stock.data"))
    return stock_list


def get_security_master():
    security_master = pandas.read_csv(
        os.path.join(DATA_DIR, "strong_oak_security_master.csv")
    )
    return security_master


def get_attributes():
    attributes = pandas.read_csv(os.path.join(DATA_DIR, "attributes.data"))
    return attributes


def generate_security_upload(
    security_master, full_stock_data, exchange_data
) -> pandas.DataFrame:
    """
    Requirements:
    Following identifiers required: MIC, QUEUESIP, Symbol, RequestId
    Either one of QUEUESIP or Symbol must be populated.
    It is important to include MIC as part of the identifiers.

    We'll start by looking at the data and cleaning up a little.
    The ticker/symbol label should be cleared up, I will use 'Symbol' as the common label and ditch 'Ticker'.
    There are lowercase 'p's spread throughout the Symbols, I will remove these.
    I would probably check with the client that all the QUEUESIP and Symbols are valid (e.g. there are some Symbols with '/' - is this valid?
    Current assumption made that it is valid.)

    Then I will start by filtering North American Securities.

    First the full_stock_data is filtered to return just North American stock list (na_stock_list).
    Then, NA Symbols and Queuesips dataframes are generated from na_stock_list.
    We need the MIC value into the security_master before filtering, so we merge the MIC values into security_master.
    filtered_security_master is the security_master ("strong_oak_security_master) which has removed all securities that are not from North America
    based on the Symbol or QUEUESIP.

    Valid securities are identified as those with either a QUEUESIP or Symbol that is not empty.

    Each of these will be assigned an EulerId, which should be an iterator starting at 1, that is the first security uploaded will be 1, the second 2, and the nth
    security designated n. An example csv upload file would be:
    """

    security_master = get_security_master()
    full_stock_data = get_stock_list()
    exchange_data = get_exchange_data()

    def remove_p_from_ticker(ticker):
        if pandas.notna(ticker):  # Check if the ticker is not NaN
            return ticker.replace("p", "")
        return ticker

    security_master["Symbol"] = security_master["Ticker"].apply(remove_p_from_ticker)

    def filter_north_american_securities(full_stock_data, exchange_data):
        north_american_exchanges = exchange_data[
            (exchange_data["domicile"] == "United States")
            | (exchange_data["domicile"] == "Canada")
        ]["MIC"].tolist()
        return full_stock_data[full_stock_data["MIC"].isin(north_american_exchanges)]

    na_stock_list = filter_north_american_securities(full_stock_data, exchange_data)
    na_stock_symbol = na_stock_list["Symbol"].dropna().unique()
    na_stock_queuesip = na_stock_list["QUEUESIP"].dropna().unique()

    security_master = pandas.merge(
        security_master,
        na_stock_list[["Symbol", "QUEUESIP", "MIC"]],
        left_on=["Symbol", "QUEUESIP"],
        right_on=["Symbol", "QUEUESIP"],
        how="left",
    )

    security_master.drop(columns=["Ticker"], inplace=True)

    filtered_security_master = security_master[
        (security_master["Symbol"].isin(na_stock_symbol))
        | (security_master["QUEUESIP"].isin(na_stock_queuesip))
    ]

    def filter_valid_securities(filtered_security_master):
        valid_securities = filtered_security_master[
            (~na_stock_list["QUEUESIP"].isnull())
            | (~filtered_security_master["Symbol"].isnull())
        ]
        return valid_securities

    valid_securities = filter_valid_securities(filtered_security_master)

    def assign_euler_ids(valid_securities):
        valid_securities["EulerId"] = range(1, len(valid_securities) + 1)
        return valid_securities

    valid_securities = assign_euler_ids(valid_securities)

    # Add any other processing or return statement here as needed for generating the security upload data.
    # For example, you might concatenate dataframes or perform other transformations.
    # You should return the final DataFrame containing the security upload data.
    return valid_securities

    pass


def generate_attribute_upload(
    security_upload, attribute_data, full_stock_data, exchange_data
) -> pandas.DataFrame:
    pass


def data_investigation(security_upload, attribute_upload):
    pass


def main():
    security_master = get_security_master()
    full_stock_data = get_stock_list()
    exchange_data = get_exchange_data()

    # * Loading Securities into the platform * #

    # get security data...
    security_upload = generate_security_upload(
        security_master=security_master,
        full_stock_data=full_stock_data,
        exchange_data=exchange_data,
    )

    # * Uploading Attributes * #

    attribute_data = get_attributes()

    # get attribute data...
    attribute_upload = generate_attribute_upload(
        security_upload=security_upload,
        attribute_data=attribute_data,
        full_stock_data=full_stock_data,
        exchange_data=exchange_data,
    )

    # solutions go here.

    security_upload.to_csv(
        os.path.join(CURRENT_DIR, f"{FIRST_NAME}_{LAST_NAME}_section1.csv")
    )
    attribute_upload.to_csv(
        os.path.join(CURRENT_DIR, f"{FIRST_NAME}_{LAST_NAME}_section2.csv")
    )

    data_investigation(
        security_upload=security_upload, attribute_upload=attribute_upload
    )


if __name__ == "__main__":
    main()
