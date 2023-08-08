import os
import pathlib
import pandas

"""
You are welcome to use this structure as a starting point, or you can start from scratch.

The prefixed `get_` methods should not need any adjustment to read in the data.
Your solutions should be mostly contained within the prefixed `generate_` methods, and the `data_investigation()`
"""

# --- Fill in your details here ---
FIRST_NAME = ""
LAST_NAME = ""

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

    security_master is cleaned by removing any value that has no QUEUESIP or Symbol, and is split up into
    valid QUEUESIPs (valid_queuesip) and valid Symbols (missing_queuesip). These are then iteratively checked for duplicates and merged with na_stock_list
    and missing information concat-ed. 2 merges are made, one with valid_queuesip + na_stock_list, and one with missing_queuesip + na_stock_list, these are the
    concat-ed with duplicates removed.

    Finally, the EulerId is added.
    """
    # Load data
    security_master = get_security_master()
    full_stock_data = get_stock_list()
    exchange_data = get_exchange_data()

    # Removes lowercase letters
    import string

    def remove_small_letters_from_ticker(ticker):
        if pandas.notna(ticker):  # Check if the ticker is not NaN
            return ticker.translate(str.maketrans("", "", string.ascii_lowercase))
        return ticker

    security_master["Ticker"] = security_master["Ticker"].apply(
        remove_small_letters_from_ticker
    )
    security_master["Symbol"] = security_master["Ticker"].apply(
        remove_small_letters_from_ticker
    )
    full_stock_data["Symbol"] = full_stock_data["Symbol"].apply(
        remove_small_letters_from_ticker
    )

    # Drop rows where both QUEUESIP and Symbol are NaN
    security_master = security_master.dropna(subset=["QUEUESIP", "Symbol"], how="all")

    # Filter rows where QUEUESIP is NaN - this will become 1/2 dataframes to merge
    missing_queuesip = security_master[security_master["QUEUESIP"].isna()]

    # Filter rows where QUEUESIP is not NaN - this will become 2/2 dataframes to merge
    valid_queuesip = security_master[security_master["QUEUESIP"].notna()]

    def filter_north_american_securities(full_stock_data, exchange_data):
        north_american_exchanges = exchange_data[
            (exchange_data["domicile"] == "United States")
            | (exchange_data["domicile"] == "Canada")
        ]["MIC"].tolist()
        return full_stock_data[full_stock_data["MIC"].isin(north_american_exchanges)]

    na_stock_list = filter_north_american_securities(full_stock_data, exchange_data)

    # First Merge with NA Stock List + Valid Queuesips

    merged_data_1 = pandas.merge(
        na_stock_list[["RequestId", "Symbol", "QUEUESIP", "MIC"]],
        valid_queuesip[["QUEUESIP", "Symbol"]],
        on="QUEUESIP",
        how="inner",
    )

    # Combine Symbol_x and Symbol_y into a single Symbol column
    merged_data_1["Symbol"] = merged_data_1.apply(
        lambda row: row["Symbol_x"]
        if pandas.notna(row["Symbol_x"])
        else row["Symbol_y"],
        axis=1,
    )

    # Drop Symbol_x and Symbol_y columns
    merged_data_1 = merged_data_1.drop(columns=["Symbol_x", "Symbol_y"])

    # Remove duplicate row with same Symbol in missing_queuesip already added in merged_data_1
    missing_queuesip = missing_queuesip[
        ~missing_queuesip["Symbol"].isin(merged_data_1["Symbol"])
    ]

    # Second Merge with NA Stock List + Missing Queuesips

    merged_data_2 = pandas.merge(
        na_stock_list[["RequestId", "Symbol", "QUEUESIP", "MIC"]],
        missing_queuesip[["QUEUESIP", "Symbol"]],
        on="Symbol",
        how="inner",
    )

    # Combine QUEUESIP_x and QUEUESIP_y into a single QUEUESIP column
    merged_data_2["QUEUESIP"] = merged_data_2.apply(
        lambda row: row["QUEUESIP_x"]
        if pandas.notna(row["QUEUESIP_x"])
        else row["QUEUESIP_y"],
        axis=1,
    )

    # Drop QUEUESIP_x and QUEUESIP_y columns
    merged_data_2 = merged_data_2.drop(columns=["QUEUESIP_x", "QUEUESIP_y"])

    # Merge merged_data_1 and merged_data_2 using pd.concat()
    merged_data = pandas.concat([merged_data_1, merged_data_2], ignore_index=True)

    # Add Euler ID

    def assign_euler_ids(merged_data):
        merged_data["EulerId"] = range(1, len(merged_data) + 1)
        return merged_data

    assign_euler_ids(merged_data)

    # Add any other processing or return statement here as needed for generating the security upload data.
    # For example, you might concatenate dataframes or perform other transformations.
    # You should return the final DataFrame containing the security upload data.
    return merged_data

    pass


def generate_attribute_upload(
    security_upload,
    attribute_data,
    exchange_data,
    security_master,
    full_stock_data,
) -> pandas.DataFrame:
    import string

    def remove_small_letters_from_ticker(ticker):
        if pandas.notna(ticker):  # Check if the ticker is not NaN
            return ticker.translate(str.maketrans("", "", string.ascii_lowercase))
        return ticker

    security_master["Symbol"] = security_master["Ticker"].apply(
        remove_small_letters_from_ticker
    )

    # START - GET STRONG OAK ID
    # Create a list of EulerId, QUEUESIP and Symbol values from security_upload
    queuesip_symbol_df = security_upload[["QUEUESIP", "Symbol", "EulerId"]]

    # Create an empty list to store the results
    strong_rows = []

    # Loop through each row in queuesip_symbol_df
    for index, row in queuesip_symbol_df.iterrows():
        queuesip = row["QUEUESIP"]
        symbol = row["Symbol"]
        euler_id = row["EulerId"]

        # Find the Strong Oak ID based on QUEUESIP or Symbol
        strong_oak_id = security_master.loc[
            (security_master["QUEUESIP"] == queuesip)
            | (security_master["Symbol"] == symbol),
            "Strong Oak Identifier",
        ].values[0]

        # Append the result to the list
        strong_rows.append(
            {
                "EulerId": euler_id,
                "QUEUESIP": queuesip,
                "Symbol": symbol,
                "Strong Oak Identifier": strong_oak_id,
            }
        )

    # Create a new DataFrame with Strong Oak ID Info
    strong_df = pandas.DataFrame(strong_rows)
    # END - GET STRONG OAK ID

    # START - GET ASSET CLASS, INCEPTION DATE, SECURITY NAME, RETURN SINCE INCEPTION
    # # Create a list of RequestId and EulerId values from security_upload
    unique_request_ids = security_upload[["RequestId", "EulerId"]]
    print(unique_request_ids)

    # Initialize lists to store attribute values
    euler_id_list = []
    asset_class_list = []
    inception_date_list = []
    security_name_list = []
    return_since_inception_list = []

    # Loop through each row in unique_request_ids
    for index, row in unique_request_ids.iterrows():
        request_id = row["RequestId"]
        euler_id = row["EulerId"]
        # Get attribute values based on RequestId
        asset_class = attribute_data.loc[
            attribute_data["RequestId"] == request_id, "Asset Class"
        ].values[0]
        inception_date = attribute_data.loc[
            attribute_data["RequestId"] == request_id, "Inception Date"
        ].values[0]
        security_name = attribute_data.loc[
            attribute_data["RequestId"] == request_id, "Security Name"
        ].values[0]
        return_since_inception = attribute_data.loc[
            attribute_data["RequestId"] == request_id, "Return Since Inception"
        ].values[0]

        # Append attribute values to lists
        euler_id_list.append(euler_id)
        asset_class_list.append(asset_class)
        inception_date_list.append(inception_date)
        security_name_list.append(security_name)
        return_since_inception_list.append(return_since_inception)

    # Create a DataFrame from the lists
    attributes_df = pandas.DataFrame(
        {
            "EulerId": euler_id_list,
            "Asset Class": asset_class_list,
            "Inception Date": inception_date_list,
            "Security Name": security_name_list,
            "Return Since Inception": return_since_inception_list,
        }
    )
    # END - GET ASSET CLASS, INCEPTION DATE, SECURITY NAME, RETURN SINCE INCEPTION

    # START - GET Exchange Name and Exchange Location
    # # Create a list of MIC and EulerId values from security_upload
    mic_list = security_upload[["MIC", "EulerId"]]

    # Initialize an empty list to store rows
    mic_rows = []

    # Loop through each row in mic_list
    for index, row in mic_list.iterrows():
        mic = row["MIC"]
        exchange_name = exchange_data.loc[exchange_data["MIC"] == mic, "name"].values[0]
        domicile = exchange_data.loc[exchange_data["MIC"] == mic, "domicile"].values[0]
        city = exchange_data.loc[exchange_data["MIC"] == mic, "city"].values[0]
        exchange_location = f"{domicile} - {city}"

        mic_rows.append(
            [mic, exchange_name, exchange_location, row["EulerId"]]
        )  # Include EulerId

    # Create a DataFrame from the rows
    mic_exchange_df = pandas.DataFrame(
        mic_rows, columns=["MIC", "Exchange Name", "Exchange Location", "EulerId"]
    )
    # END - GET Exchange Name and Exchange Location

    # START - MERGER
    # Merge mic_exchange_df with attributes_df on EulerId
    first_merge = mic_exchange_df.merge(attributes_df, on="EulerId", how="inner")

    # Merge first_merge with strong_df on EulerId
    final_merge = first_merge.merge(strong_df, on="EulerId", how="inner")

    # Remove QUEUESIP, Symbol, and MIC columns
    final_merge = final_merge.drop(columns=["QUEUESIP", "Symbol", "MIC"])

    # Reorder columns with EulerId as the first column
    merged_data_2 = final_merge[
        ["EulerId"] + [col for col in final_merge.columns if col != "EulerId"]
    ]
    # END - MERGER

    # Initialize an empty list to store rows
    final_rows = []

    # Loop through each row in merged_data_2
    for index, row in merged_data_2.iterrows():
        euler_id = row["EulerId"]

        # Iterate through columns other than EulerId
        for column in merged_data_2.columns[1:]:
            attribute_name = column
            attribute_value = row[column]

            # Check if AttributeValue is NaN, if not, append the attribute row
            if pandas.notna(attribute_value):
                final_rows.append([euler_id, attribute_name, attribute_value])

    # Create a DataFrame from the rows
    final_dataframe = pandas.DataFrame(
        final_rows, columns=["EulerId", "AttributeName", "AttributeValue"]
    )

    return final_dataframe

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
        security_master=security_master,
    )

    # solutions go here.

    """
    The client has also asked for an explanation of their dataset. They would like to know:
    Q: How many valid securities they have compared to the stock list? How many securities are unable to be loaded into the platform?
    A: Out of the 1835 securities, limiting down to North American securities and then checking for valid securities, there are now 970 valid securities.

    Q: What is the count of each of the Asset Classes in their dataset that will exist in the platform, after uploading the "attributes" file?
    A: Out of the 970 securities the counts of Asset Classes are
        Domestic Equity         339
        International Equity    327
        ETF                     220
    
    Q: Comment on which of the attribute values should be non-nullable. i.e. which attributes should always be populated and why? 
    Consider the requirements of Section 1, and the implications of that on the attributes.
    A: The EulerId should always be populated as it is a count, either QUEUESIP, Symbol or Security Name in the Exchange Name needs to be identified. 
    These are the identifiers in which hopefully all other variables should be obtainable through research.

    
    """
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
