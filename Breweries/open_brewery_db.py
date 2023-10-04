from requests import get

class OpenBreweryDB:
	def __init__(self) -> None:
		self.api = "https://api.openbrewerydb.org"
		self.headers = {
			"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36",
		}
	
	def get_single_brewery(self, brewery_id: str) -> dict:
		return get(
			f"{self.api}/breweries/{brewery_id}",
			headers=self.headers).json()
	
	def get_breweries_list(
			self,
			page: int,
			limit: int) -> dict:
		return get(
			f"{self.api}/breweries?page={page}&per_page={limit}",
			headers=self.headers).json()
	
	def get_breweries_by_city(
			self,
			city: str,
			page: int,
			limit: int) -> dict:
		return get(
			f"{self.api}/breweries?by_city={city}&page={page}&per_page={limit}",
			headers=self.headers).json()
	
	def get_breweries_by_dist(
			self,
			dist: str,
			page: int,
			limit: int) -> dict:
		return get(
			f"{self.api}/breweries?by_dist={dist}&page={page}&per_page={limit}",
			headers=self.headers).json()
			
	def get_breweries_by_name(
			self,
			name: str,
			page: int,
			limit: int) -> dict:
		return get(
			f"{self.api}/breweries?by_name={name}&page={page}&per_page={limit}",
			headers=self.headers).json()
	
	def get_breweries_by_state(
			self,
			state: str,
			page: int,
			limit: int) -> dict:
		return get(
			f"{self.api}/breweries?by_state={state}&page={page}&per_page={limit}",
			headers=self.headers).json()
	
	def get_breweries_by_postal(
			self,
			postal: int,
			page: int,
			limit: int) -> dict:
		return get(
			f"{self.api}/breweries?by_postal={postal}&page={page}&per_page={limit}",
			headers=self.headers).json()
	
	def get_breweries_by_type(
			self,
			type: str,
			page: int,
			limit: int) -> dict:
		return get(
			f"{self.api}/breweries?by_type={type}&page={page}&per_page={limit}",
			headers=self.headers).json()
	
	def get_random_brewery(self) -> dict:
		return get(
			f"{self.api}/breweries/random",
			headers=self.headers).json()
	
	def search_breweries(self, query: str) -> dict:
		return get(
			f"{self.api}/breweries/search?query={query}",
			headers=self.headers).json()