import datetime


class State:
    """Класс для хранения временной информации тестов, удобно использовать в качестве глобальных переменных"""

    vault = None
    """Объект типа Vault"""

    token: str = None
    token_expires_in: datetime = None

    campaigns: list = []
    """Созданные в ходе тестов кампании"""

    app_ids: list = []
    """Созданные в ходе тестов приложения в справочнике dict.app_list"""

    domain_filters: list = []
    """Созданные в ходе тестов доменные фильтры"""

    custom_filters: list = []
    """Созданные в ходе тестов пользовательские фильтры"""

    host_names: list = []
    """Созданные в ходе тестов технических хостов в справочнике dict.hosts_dictionary"""

    hosts_embedding: list = []
    """Созданные в ходе тестов хосто/звонков в справочнике dict.embedding"""

    client_id: str = None
    client_secret: str = None

    admin_user: str = None
    admin_password: str = None

    prep_user: str = None
    prep_password: str = None

    test_user: str = None
    test_password: str = None

    imsi_list: list = []
    msisdn_list: list = []
    guid_list: list = []
    imei_list: list = []

    is_gitlab_runner: bool = False

    keytab_base64_prep = None

    keytab_base64_test = None

    aerospike_entities: list = []
