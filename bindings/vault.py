import hvac
import urllib3

from common.bindings.env import Env


class Vault:

    def __init__(self, host="https://vault.mts-corp.ru"):

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.env = Env()

        self.host = host
        self.token = self.env.get_env_data_by_name(name="CICD_DR__VAULT_ROLE_TOKEN")

        # Если тест запускается из GitLab, то у нас будет токен
        if self.token is not None:
            self.client = hvac.Client(url=self.host, token=self.token, verify=False)

        # Если тест запускается локально, то токена не будет и понадобятся role_id и secret_id из env-файла
        else:
            self.role_id = self.env.get_env_data_by_name(name="VAULT_ROLE_ID")
            self.secret_id = self.env.get_env_data_by_name(name="VAULT_SECRET_ID")

            self.client = hvac.Client(url=self.host, verify=False)
            self.authenticate()

        # Проверка успешного подключения
        if not self.client.is_authenticated():
            raise ConnectionError("Ошибка аутентификации. Проверьте токен или URL.")

    def authenticate(self):
        """Аутентификация через AppRole (role_id + secret_id)"""
        try:
            response = self.client.auth.approle.login(
                role_id=self.role_id,
                secret_id=self.secret_id
            )
            self.client.token = response['auth']['client_token']
        except Exception as e:
            raise ConnectionError(f"Ошибка аутентификации через AppRole: {e}")

        # Проверка успешного подключения
        if not self.client.is_authenticated():
            raise ConnectionError("Ошибка аутентификации. Проверьте role_id/secret_id или URL.")

    def get_secret_value(self, path: str, key: str):
        try:
            vault_dir = self.client.read(path)
        except Exception as e:
            raise ConnectionError(f'Нет прав для чтения: {path}. {e}')

        if vault_dir:
            password = vault_dir['data']['data'].get(key)
            if not password:
                raise ValueError(f'Секрет ({key}) не найден по указанному пути: {path}')
            return password
        else:
            raise ValueError(f'Путь ({path}) не существует')