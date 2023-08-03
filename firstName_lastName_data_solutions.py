import os
import pathlib

import pandas

"""You are welcome to use this structure as a starting point, or you can start from scratch.

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
    """A summary should go here of what this function does. You should include documentation like this on all your
    functions, describing inputs, and outputs...
    """

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
