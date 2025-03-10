# from django.test import TestCase
import unittest
from django.urls import reverse
from django.test import Client


class TestUserNotificationDetails(unittest.TestCase):
	def setUp(self):
		self.client = Client()

	def test_user_notification_success(self):
		response = self.client.get('/python/user/notification?from=0&to=10',
								   HTTP_AUTHORIZATION="Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.hSW73YjKdlnnV8pCBLF630gz5FDyyhRAKoAkeMgo1vnU02zXuH8noZxKr_l6PvNnrdE9EBXVgPaXH9xo8gzsDdZ4UEQ3d1OO-oeMAcEZnlURX-zFBS0VJXKXC6kGIHCRNwIIdFbwQ0672QK72Fet2xdJMCvD_yCtU8oBQy3qomk.vEzPshRJOgUXoa3Z.ZfUqs7nGhow6nQS3As8gIwBuWZDxKazDtLD4KoFZYoOpg91M-CTshvnKHlNYymUoNtk3Je0D8b9YNtnk0YCRvQGTi4JeaQV70FOnMup05VFRoSOwHV6ZyIcaYOtDrHYhaXQJO0KCU3WKPxYtqqt-wIZPTLG6JhNpoQss0Rw8zbp6XixtwMuRXrHj2Q1YYJEPZ4MVS6Yz7tjtToE9WdRo0uodToIe0tgej_WAI4LsmJ8GBGmz2IQXED1xrHF_yPqzli_woQEB8YjBWeBC3Op_glYDKOjOUsFqmv1NwoHMafnmEh65L0rfyhJbqQDSKEck4xcEX0BjHSjJiKNmtQL4AAUTR_fNa8Q_goru0aTZxokJGytkUMEi3DXN7FbSNiN5VEeeD0zZ3XqyUHlqxtouFXKS4uvLN1mKgKLflmHoIWeory80tvlh7l6apDcIydV-SrceJU1iO_4zlDYgqBK4_7sS069iKUVKAofiKxgH-YF8iyO5H9z61BzfNKZFKEv5rC49t_9qIp1arXcDiKlGp3Kqm0pUn0mL6W40XVL0uMAGU5fseFfVlBgfOnLbhYTudbzdpMod-nkHUK9n-mdfxRViEofO52hR5R4y4ZbzqJw5KB2RsMOoliyz8lrd0FPfPhiNa0RCeep_ytscjMEDY-ImzkFs_ZF6o1N7S-eoGQTAlOlUllxgSiuXIhdMNHEOlvaqzGzrnY0nZkc._7TeQNAa-_dQEVGQT8mJww",
								   HTTP_APPNAME="Shoppd")
		self.assertEqual(response.status_code, 200)
		self.assertGreater(len(response.json()['data']), 0)

	def test_user_notification_not_found(self):
		response = self.client.get('/python/user/notification?from=0&to=10',
								   HTTP_AUTHORIZATION="Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.hSW73YjKdlnnV8pCBLF630gz5FDyyhRAKoAkeMgo1vnU02zXuH8noZxKr_l6PvNnrdE9EBXVgPaXH9xo8gzsDdZ4UEQ3d1OO-oeMAcEZnlURX-zFBS0VJXKXC6kGIHCRNwIIdFbwQ0672QK72Fet2xdJMCvD_yCtU8oBQy3qomk.vEzPshRJOgUXoa3Z.ZfUqs7nGhow6nQS3As8gIwBuWZDxKazDtLD4KoFZYoOpg91M-CTshvnKHlNYymUoNtk3Je0D8b9YNtnk0YCRvQGTi4JeaQV70FOnMup05VFRoSOwHV6ZyIcaYOtDrHYhaXQJO0KCU3WKPxYtqqt-wIZPTLG6JhNpoQss0Rw8zbp6XixtwMuRXrHj2Q1YYJEPZ4MVS6Yz7tjtToE9WdRo0uodToIe0tgej_WAI4LsmJ8GBGmz2IQXED1xrHF_yPqzli_woQEB8YjBWeBC3Op_glYDKOjOUsFqmv1NwoHMafnmEh65L0rfyhJbqQDSKEck4xcEX0BjHSjJiKNmtQL4AAUTR_fNa8Q_goru0aTZxokJGytkUMEi3DXN7FbSNiN5VEeeD0zZ3XqyUHlqxtouFXKS4uvLN1mKgKLflmHoIWeory80tvlh7l6apDcIydV-SrceJU1iO_4zlDYgqBK4_7sS069iKUVKAofiKxgH-YF8iyO5H9z61BzfNKZFKEv5rC49t_9qIp1arXcDiKlGp3Kqm0pUn0mL6W40XVL0uMAGU5fseFfVlBgfOnLbhYTudbzdpMod-nkHUK9n-mdfxRViEofO52hR5R4y4ZbzqJw5KB2RsMOoliyz8lrd0FPfPhiNa0RCeep_ytscjMEDY-ImzkFs_ZF6o1N7S-eoGQTAlOlUllxgSiuXIhdMNHEOlvaqzGzrnY0nZkc._7TeQNAa-_dQEVGQT8mJww", HTTP_APPNAME="Shoppd")
		self.assertEqual(response.status_code, 404)
		self.assertEqual(len(response.json()['data']), 0)
		self.assertNotEqual((len(response.json()['data'])), 1)

	def test_user_notification_error(self):
		response = self.client.get('/python/user/notification?from=0&to=10',
								   HTTP_APPNAME="Shoppd")
		self.assertEqual(response.status_code, 500)
		self.assertEqual(len(response.json()['data']), 0)
		self.assertNotEqual((len(response.json()['data'])), 1)

	def test_user_notification_unauthorized(self):
		response = self.client.get('/python/user/notification?from=0&to=10',
								   HTTP_AUTHORIZATION="", HTTP_APPNAME="Shoppd")
		self.assertEqual(response.status_code, 401)
		self.assertEqual(len(response.json()['data']), 0)
		self.assertNotEqual((len(response.json()['data'])), 1)