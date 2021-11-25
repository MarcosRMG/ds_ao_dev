from etl import HmMensJeans


def main():
    # hm object
    hm = HmMensJeans()

    # Job 1
    hm.url_full_page()
    hm.product_base()

    # Job 2
    hm.product_details()

    # Job 3
    hm.data_cleaning()

    # Job 4
    hm.database()

if __name__ == '__main__':
    main()