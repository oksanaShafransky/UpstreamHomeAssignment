class VehicleFetchException(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(f'{self.message}. Status code {status_code}')