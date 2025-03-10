from django.test import TestCase
from django.test import Client
import json
client = Client()
import requests

# Create your tests here.

class TestSearchFilter(TestCase):
    ''' Test product listing page API with all scenario '''

    def test_perfect_data(self):
        ''' all data are perferct so API give 200 '''
        url = "https://api.shoppd.net/python/search/filter/new?cityId=5df7b7218798dc2c1114e6bf&page=1&sort=price_asc&fname=5df840532f39cd6b2ff0b5ec&sname=5df840562f39cd6b2ff0b5fd&integrationType=0&productType=1"

        payload={}
        headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.AVvcs_StFM0qZqE7NhChEY47vYnLVdLR0CR0I074rqNlX7nGVNfgpWXNx148JE0WOQwALvUaYCG2iUONnDjp01l6XGuewJ_ixq1Fhf5t3OGj815TK0ksmaf_C-wXCS6flgtpAYJ5zMYbCy1uAys3QUXvBZh59oQLWKNR_Lyh8Fo.HKMl1TptIXXPaGx6.A3JzKlIx6EiqUJB2Af9QQ3JdPd4e1OSOXhEdFuuw64vGLXk29RKo5fDSOomSKGA3bpXvzXuY7C_HM0_OABg9vyDdHl3xWYRnbiP2clAj-unZSbYxdk033egX1npqTzMTcCdPuJIfGlsEmnTTcp29Y94Doz3IxPSFHSGF0IDS1Hdjtwx01hUnCf14yCJ87gaDq4qRTqjoBBhCAO_jFCou6XKhGu43GQo7M275rhLJ6-Q4sFM8CmVq5BabGlfAv99vgFoGRQ0kTg_IoimZhJmhKdZ_mbCGWebtxmCzAt9CbpnGNQYMD2xYoccHyNTOJSx7SEaWnVolKNOky5RAhw-Wsp8RepF-JJheU0K8psz0TQHuH3nB1gqZdeDVzjbB6YWJw0UUtGtP-nm9HaiQCoG1rORoKFVOCJcrBiwok67Y7RWGJFZtN5fhUWAYhXYmWfwqeJy6mxMXF0IMYt6EF4Rl687Bb4IoGzVuVRKIwUqlaNu9aHht-d37tjWBVSj8o8uF4WIImvpuuRPBYq6o17LgkeNgXc21fHkh28wCoKMw83ROZHrGFHCmd__QHIhapOYjY8sPpVWXbb38RyhcDI9ap8DutQ_cQnQ3MZ5IwJdS9b4DhjA4JR65A4GE6kR32kteKOk6EuZe3ZG2t368Xx53Py2o_A_urhuo7BzNUvM_1TBNOuHbMLC6D_c56DJ9NCetr9Qqbla1d8iU_irF6iKNwHfJWJVKwtmq-4c2Ff5W7KfniT19RG_ygglzIAPbBFfH-061UeprcmqEKOTvQnH9g8C_wH26l6z6tR2mfBA9Er5KRJGOuuX6N4E0G9uyTwlshYce2CuacRFHpQj9vy0BQbAq4ZcHEoeYdneDiEd1bCv3iuXrgSgr4H0KUeedMyJLBeC3gzw-27G_xtPJ8ibmZI0eSJ_p2aRI7TlR778tMgpHeRFf-LbNc_odpjZ46YmhRhYk-GvzaJ9AIj9PW1ksPuHIcFhZQBmqVc0rCPjrDHzDVjASYgU_MNYBGu_Ultaf_14S9rrSNJ_wBqPrIvr-p6LJfeOVArhV06K3gkoEfUlNDzc2W53F9mC8fe91m9PelYHfp2609ouN7MwjAxV3e7E93DFiVyeXsfzSeYkjUZ7gwqRU_e2sN9Y_tzY4eoLEnIvzkTnan7-08lF5SS29lqq48GKOTN6cpx7Qziv1H_1wbJxkMjMvJFLblGyt9tuhnsFVFoLIo6_aiTcrN3erTe1DSs9AiegteMuvFTLXrcEyLfu5e3FwqWnFmaAPBZwkrm-WUnxensMHODpYSqdj5bFtL-GsnoBX_28xSuXjMpR7RnVLvmRIoUo3PCf-3AeBJnSrO8a7JEq_CtdCvGbLAemG7zN0hMuGas3tDsOJs52V5z-HGP8GM-Fc9A_46qsB5Y2BaVa0AUpt_UQn-Wa-nV_rgYT2JxwwcDtKl0plm8DQhtesTD1EoI4Vhwas-6oCPVkdfkjdH6zw_R8W9wR_Ne2jOq7XE1O7MxqqzanmrpcRHsALpYJyVIjfEdCO4oBBiV4y2awdl5z4roqmJr6SqlcjsP4wpNJFf5hM08EW5FLwyf0sCEEQ5dSIeoznfzsi7-DM2dl87Rc12xtCF0rgZp6HoWue-7hBLm2Hsi4nTXcSE4Y1YJeQVkudprpDzqqRIKGuV5jocuId-eOb_FKoRD9vUtgRaMLLI7A6EPzRbyG63xLdKxsKhOpk-GMJdQ6xv4gzuQ.rHyTJcLWzgQjSuPn3BpZVg',
        'language': 'en',
        'currencycode': 'INR',
        'loginType': '1',
        'ipAddress': '124.40.244.94',
        'platform': '0',
        'city': '5df7b7218798dc2c1114e6bf',
        'country': '5df7b7218798dc2c1114e6c0',
        'searchType': '1',
        'latitude': '12.9716',
        'longitude': '77.5946',
        'storeCategoryId': '5cc0846e087d924456427975'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        print(response.json())
        self.assertEqual(response.status_code, 200)
    
    def test_unauthorized_401_data(self):
        ''' if you pass empty value of authorization '''

        url = "https://api.shoppd.net/python/search/filter/new?cityId=5df7b7218798dc2c1114e6bf&page=1&sort=price_asc&fname=5df840532f39cd6b2ff0b5ec&sname=5df840562f39cd6b2ff0b5fd&integrationType=0&productType=1"

        payload={}
        headers = {
        'accept': 'application/json',
        'Authorization': '',
        'language': 'en',
        'currencycode': 'INR',
        'loginType': '1',
        'ipAddress': '124.40.244.94',
        'platform': '0',
        'city': '5df7b7218798dc2c1114e6bf',
        'country': '5df7b7218798dc2c1114e6c0',
        'searchType': '1',
        'latitude': '12.9716',
        'longitude': '77.5946'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 401)

    def test_data_missing(self):
        ''' if send mendetory field empty EX: StoreCategoryId '''

        url = "https://api.shoppd.net/python/search/filter/new?cityId=5df7b7218798dc2c1114e6bf&page=1&sort=price_asc&fname=5df840532f39cd6b2ff0b5ec&sname=5df840562f39cd6b2ff0b5fd&integrationType=0&productType=1"
        payload={}
        headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.gByQGPBpSjq_02ouEMm1NPVi0ztFADiTZuPVquOsYBxbUnp1K29duTWG9UoxPDjaOmr4Y5xbJSlanyKzpolaFuJcj2nOD6yaOhaUttHLgDwxmZCDhKUn21IjxTPndI0Bq0bgNydrMom51pkr9HQbpvl4HwYeU_K4Fp8-hF_3nno.ooQk1k0R_zz_B05H.xiLEpsVzQnmsEhGz_dxAcubQYS_VSlGjXfn0cbPFvqEAftKf6kzXAXTPIgAUu6OmzTW_GC3V8jJA0iurGofo7lBxnnLO7lFQ5imd00riERSp54m5DpyaOP0z54ilTJNHPHioDkRmo7pRXMLmQ02twbIh75ndi4loYCwkU6ElDF6Vlw4MQhaFQEZAW5_Ki-vw4So5y-HVa6rGHnrFkZonNjEaYw1JuqynB6UfG_VMOJk-lEfnHFQCDP_TpTitB5LkIGj9-m2CuunAUhTbGmmvOk3QnlTv_L2Pb5Fv7s4pqPmMJNFVyLBNSUdVrIIBJBop9yxYqTIZi4oDnXXfS7rp4EDPbQYePgTmgg5s9rs8MJR2LmDaReBvyn3Sv3aUnK1L7mHHexei9g-5tB-akfIGvXVsannni5Y68vnVfNLzfKVISVf0q-kLYthFE5BIX4pdVPAfTQlaIfxqsiqSmbsVaxbxmRS5Z6VV3FFUi4kpNl5b6p5nJPDAiBQLZw5m-KnpM6lTqWDFpJswiytg5NCwiHmAo6isa-AS5-ObYXgPEFTwInj8Xnux_EVdWwiQUXHzdFjtZfKjK2qxo6eaoV29RAdqSQIVyKUlrreT4tXh5jkr9HCmf9X9di5jfsko30mBGYKxXFOpchiMOakvm_mZ6-yOwuSKW083TaXN7yvUA_PiIabXDTHun1qsFgSi7WDqvLmzRti2T4LWE82gYJEXJdWmCw3B7Iw9ZXCiLH5N9ge0jp6Z9wKtk9cwG3h4Hzuc0qK59PU1SAph6a-eHmOkeOJ7tqBBH0Q02HKuQ84ix-SHgo_Mm3Xs_9tauZqeIGwyqSr8M6RDSQvcXhaQKM2wZXUznzq_kybvdhzdFRWjqayY3jzHoShTirBoEcPlhzGxcUlMQdfAH30U0DAFfV5-6bQpYP0dmi_Dtfv0LD5KLgUImr-QEpuutZXhGa5kXk0cs3uAGEv2nwgkL_fMooc-9pJxXTX7af61hBxJ9yVs66ag0lqpDspbt9ZgClMlKCGbx7HW9QzgpiP4IkZOtVLMGC3xG4_9h1sGEDNlmjw--L_ba_Eu1KEVO4-ze1FwXSagC8zbcFlPcsXDVlaaD6JvWE9-tBtd69moMfDhl2yzND3QtqwwT-zxGb5GPBr8ii7spW7JdMQfs9GTsw-iwa_b9lX0Mbm-flTOHSujYJH5ZjgPKScZNN8mEc83GFLpIFfbTYtTmdu00QMxmaXj8hP-Ydf08pNSjnp9ygAnKfIV7k4ZgEolfTnFcdoyZuREJRZneQMbF4jdLECtRl8SrtwfHUWZp1t542b40_OOrD9hJ857v01YRCtQ2vTDAYxAztDXMBFbOh2H_82MUU79VAk0kdBJ9Eh2H1JsoYmBOM6AZv8ENvWZRmJG1y863GvsTa--KqWVxZyAMNrK5edg06Ys9e9739kwmVvIgjjOZqG4-UmmmGBD9aRCbTvO2RYlxWg3aP8paeo4_3e5OVtW---h5JWzfFdyyXHgnLCIhMchYq5jUz4d00kNX-Pf4E_hj_mK2-g3e79gftVxSwPJd2yIl-TXF0Hfd1uDiMkc65bFM-RRL4GmBud489QOcVqNzpnTnVK78vQB-uMMxZik9HIHolvIT1m1Hg5pAhNcmsmN7yh9SwepUkdTRDVYzLWVHcpjw05-1sjpUg28Dw4P7_yzLf9-aJ5XwHfjr_02jrzDXFh13GFPAKUOPOUvktoJhOlRkySamg.n9KU0ACmveGQTOaCvhRVUw',
        'language': 'en',
        'currencycode': 'INR',
        'loginType': '1',
        'ipAddress': '124.40.244.94',
        'platform': '0',
        'city': '5df7b7218798dc2c1114e6bf',
        'country': '5df7b7218798dc2c1114e6c0',
        'searchType': '1',
        'latitude': '12.9716',
        'longitude': '77.5946'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 404)

    def test_500_error_data(self):
        ''' if did not send mendetory field Ex: StoreCategoryId '''

        url = "https://api.shoppd.net/python/search/filter/new?cityId=5df7b7218798dc2c1114e6bf&page=1&sort=price_asc&fname=5df840532f39cd6b2ff0b5ec&sname=5df840562f39cd6b2ff0b5fd&integrationType=0&productType=1"
        payload={}
        headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.gByQGPBpSjq_02ouEMm1NPVi0ztFADiTZuPVquOsYBxbUnp1K29duTWG9UoxPDjaOmr4Y5xbJSlanyKzpolaFuJcj2nOD6yaOhaUttHLgDwxmZCDhKUn21IjxTPndI0Bq0bgNydrMom51pkr9HQbpvl4HwYeU_K4Fp8-hF_3nno.ooQk1k0R_zz_B05H.xiLEpsVzQnmsEhGz_dxAcubQYS_VSlGjXfn0cbPFvqEAftKf6kzXAXTPIgAUu6OmzTW_GC3V8jJA0iurGofo7lBxnnLO7lFQ5imd00riERSp54m5DpyaOP0z54ilTJNHPHioDkRmo7pRXMLmQ02twbIh75ndi4loYCwkU6ElDF6Vlw4MQhaFQEZAW5_Ki-vw4So5y-HVa6rGHnrFkZonNjEaYw1JuqynB6UfG_VMOJk-lEfnHFQCDP_TpTitB5LkIGj9-m2CuunAUhTbGmmvOk3QnlTv_L2Pb5Fv7s4pqPmMJNFVyLBNSUdVrIIBJBop9yxYqTIZi4oDnXXfS7rp4EDPbQYePgTmgg5s9rs8MJR2LmDaReBvyn3Sv3aUnK1L7mHHexei9g-5tB-akfIGvXVsannni5Y68vnVfNLzfKVISVf0q-kLYthFE5BIX4pdVPAfTQlaIfxqsiqSmbsVaxbxmRS5Z6VV3FFUi4kpNl5b6p5nJPDAiBQLZw5m-KnpM6lTqWDFpJswiytg5NCwiHmAo6isa-AS5-ObYXgPEFTwInj8Xnux_EVdWwiQUXHzdFjtZfKjK2qxo6eaoV29RAdqSQIVyKUlrreT4tXh5jkr9HCmf9X9di5jfsko30mBGYKxXFOpchiMOakvm_mZ6-yOwuSKW083TaXN7yvUA_PiIabXDTHun1qsFgSi7WDqvLmzRti2T4LWE82gYJEXJdWmCw3B7Iw9ZXCiLH5N9ge0jp6Z9wKtk9cwG3h4Hzuc0qK59PU1SAph6a-eHmOkeOJ7tqBBH0Q02HKuQ84ix-SHgo_Mm3Xs_9tauZqeIGwyqSr8M6RDSQvcXhaQKM2wZXUznzq_kybvdhzdFRWjqayY3jzHoShTirBoEcPlhzGxcUlMQdfAH30U0DAFfV5-6bQpYP0dmi_Dtfv0LD5KLgUImr-QEpuutZXhGa5kXk0cs3uAGEv2nwgkL_fMooc-9pJxXTX7af61hBxJ9yVs66ag0lqpDspbt9ZgClMlKCGbx7HW9QzgpiP4IkZOtVLMGC3xG4_9h1sGEDNlmjw--L_ba_Eu1KEVO4-ze1FwXSagC8zbcFlPcsXDVlaaD6JvWE9-tBtd69moMfDhl2yzND3QtqwwT-zxGb5GPBr8ii7spW7JdMQfs9GTsw-iwa_b9lX0Mbm-flTOHSujYJH5ZjgPKScZNN8mEc83GFLpIFfbTYtTmdu00QMxmaXj8hP-Ydf08pNSjnp9ygAnKfIV7k4ZgEolfTnFcdoyZuREJRZneQMbF4jdLECtRl8SrtwfHUWZp1t542b40_OOrD9hJ857v01YRCtQ2vTDAYxAztDXMBFbOh2H_82MUU79VAk0kdBJ9Eh2H1JsoYmBOM6AZv8ENvWZRmJG1y863GvsTa--KqWVxZyAMNrK5edg06Ys9e9739kwmVvIgjjOZqG4-UmmmGBD9aRCbTvO2RYlxWg3aP8paeo4_3e5OVtW---h5JWzfFdyyXHgnLCIhMchYq5jUz4d00kNX-Pf4E_hj_mK2-g3e79gftVxSwPJd2yIl-TXF0Hfd1uDiMkc65bFM-RRL4GmBud489QOcVqNzpnTnVK78vQB-uMMxZik9HIHolvIT1m1Hg5pAhNcmsmN7yh9SwepUkdTRDVYzLWVHcpjw05-1sjpUg28Dw4P7_yzLf9-aJ5XwHfjr_02jrzDXFh13GFPAKUOPOUvktoJhOlRkySamg.n9KU0ACmveGQTOaCvhRVUw',
        'language': 'en',
        'currencycode': 'INR',
        'loginType': '1',
        'ipAddress': '124.40.244.94',
        'platform': '0',
        'city': '5df7b7218798dc2c1114e6bf',
        'country': '5df7b7218798dc2c1114e6c0',
        'searchType': '1',
        'latitude': '12.9716'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 500)

class TestProductDetailPage(TestCase):
    ''' Test product detail page API with all scenario '''
    def test_200_perfect_data(self):
        ''' without zoneId in header '''

        url = "https://api.shoppd.net/python/product/details?productId=61ea8b077e22d539eabf0bd4&referralId=PRJC4&cityId=5df7b7218798dc2c1114e6bf&parentProductId=61ea8b057e22d539eabf0bd2&isSearch=1"

        payload={}
        headers = {
        'accept': 'application/json',
        'currencycode': 'INR',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.gByQGPBpSjq_02ouEMm1NPVi0ztFADiTZuPVquOsYBxbUnp1K29duTWG9UoxPDjaOmr4Y5xbJSlanyKzpolaFuJcj2nOD6yaOhaUttHLgDwxmZCDhKUn21IjxTPndI0Bq0bgNydrMom51pkr9HQbpvl4HwYeU_K4Fp8-hF_3nno.ooQk1k0R_zz_B05H.xiLEpsVzQnmsEhGz_dxAcubQYS_VSlGjXfn0cbPFvqEAftKf6kzXAXTPIgAUu6OmzTW_GC3V8jJA0iurGofo7lBxnnLO7lFQ5imd00riERSp54m5DpyaOP0z54ilTJNHPHioDkRmo7pRXMLmQ02twbIh75ndi4loYCwkU6ElDF6Vlw4MQhaFQEZAW5_Ki-vw4So5y-HVa6rGHnrFkZonNjEaYw1JuqynB6UfG_VMOJk-lEfnHFQCDP_TpTitB5LkIGj9-m2CuunAUhTbGmmvOk3QnlTv_L2Pb5Fv7s4pqPmMJNFVyLBNSUdVrIIBJBop9yxYqTIZi4oDnXXfS7rp4EDPbQYePgTmgg5s9rs8MJR2LmDaReBvyn3Sv3aUnK1L7mHHexei9g-5tB-akfIGvXVsannni5Y68vnVfNLzfKVISVf0q-kLYthFE5BIX4pdVPAfTQlaIfxqsiqSmbsVaxbxmRS5Z6VV3FFUi4kpNl5b6p5nJPDAiBQLZw5m-KnpM6lTqWDFpJswiytg5NCwiHmAo6isa-AS5-ObYXgPEFTwInj8Xnux_EVdWwiQUXHzdFjtZfKjK2qxo6eaoV29RAdqSQIVyKUlrreT4tXh5jkr9HCmf9X9di5jfsko30mBGYKxXFOpchiMOakvm_mZ6-yOwuSKW083TaXN7yvUA_PiIabXDTHun1qsFgSi7WDqvLmzRti2T4LWE82gYJEXJdWmCw3B7Iw9ZXCiLH5N9ge0jp6Z9wKtk9cwG3h4Hzuc0qK59PU1SAph6a-eHmOkeOJ7tqBBH0Q02HKuQ84ix-SHgo_Mm3Xs_9tauZqeIGwyqSr8M6RDSQvcXhaQKM2wZXUznzq_kybvdhzdFRWjqayY3jzHoShTirBoEcPlhzGxcUlMQdfAH30U0DAFfV5-6bQpYP0dmi_Dtfv0LD5KLgUImr-QEpuutZXhGa5kXk0cs3uAGEv2nwgkL_fMooc-9pJxXTX7af61hBxJ9yVs66ag0lqpDspbt9ZgClMlKCGbx7HW9QzgpiP4IkZOtVLMGC3xG4_9h1sGEDNlmjw--L_ba_Eu1KEVO4-ze1FwXSagC8zbcFlPcsXDVlaaD6JvWE9-tBtd69moMfDhl2yzND3QtqwwT-zxGb5GPBr8ii7spW7JdMQfs9GTsw-iwa_b9lX0Mbm-flTOHSujYJH5ZjgPKScZNN8mEc83GFLpIFfbTYtTmdu00QMxmaXj8hP-Ydf08pNSjnp9ygAnKfIV7k4ZgEolfTnFcdoyZuREJRZneQMbF4jdLECtRl8SrtwfHUWZp1t542b40_OOrD9hJ857v01YRCtQ2vTDAYxAztDXMBFbOh2H_82MUU79VAk0kdBJ9Eh2H1JsoYmBOM6AZv8ENvWZRmJG1y863GvsTa--KqWVxZyAMNrK5edg06Ys9e9739kwmVvIgjjOZqG4-UmmmGBD9aRCbTvO2RYlxWg3aP8paeo4_3e5OVtW---h5JWzfFdyyXHgnLCIhMchYq5jUz4d00kNX-Pf4E_hj_mK2-g3e79gftVxSwPJd2yIl-TXF0Hfd1uDiMkc65bFM-RRL4GmBud489QOcVqNzpnTnVK78vQB-uMMxZik9HIHolvIT1m1Hg5pAhNcmsmN7yh9SwepUkdTRDVYzLWVHcpjw05-1sjpUg28Dw4P7_yzLf9-aJ5XwHfjr_02jrzDXFh13GFPAKUOPOUvktoJhOlRkySamg.n9KU0ACmveGQTOaCvhRVUw',
        'language': 'en',
        'loginType': '1',
        'ipAddress': '124.40.244.94',
        'latitude': '12.9716',
        'longitude': '77.5946',
        'platform': '0',
        'country': 'India'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 200)
    
    def test_200_perfect_data(self):
        ''' with zoneId in header '''
        url = "https://api.shoppd.net/python/product/details?productId=61ea8b077e22d539eabf0bd4&referralId=PRJC4&cityId=5df7b7218798dc2c1114e6bf&parentProductId=61ea8b057e22d539eabf0bd2&isSearch=1"

        payload={}
        headers = {
        'accept': 'application/json',
        'currencycode': 'INR',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.gByQGPBpSjq_02ouEMm1NPVi0ztFADiTZuPVquOsYBxbUnp1K29duTWG9UoxPDjaOmr4Y5xbJSlanyKzpolaFuJcj2nOD6yaOhaUttHLgDwxmZCDhKUn21IjxTPndI0Bq0bgNydrMom51pkr9HQbpvl4HwYeU_K4Fp8-hF_3nno.ooQk1k0R_zz_B05H.xiLEpsVzQnmsEhGz_dxAcubQYS_VSlGjXfn0cbPFvqEAftKf6kzXAXTPIgAUu6OmzTW_GC3V8jJA0iurGofo7lBxnnLO7lFQ5imd00riERSp54m5DpyaOP0z54ilTJNHPHioDkRmo7pRXMLmQ02twbIh75ndi4loYCwkU6ElDF6Vlw4MQhaFQEZAW5_Ki-vw4So5y-HVa6rGHnrFkZonNjEaYw1JuqynB6UfG_VMOJk-lEfnHFQCDP_TpTitB5LkIGj9-m2CuunAUhTbGmmvOk3QnlTv_L2Pb5Fv7s4pqPmMJNFVyLBNSUdVrIIBJBop9yxYqTIZi4oDnXXfS7rp4EDPbQYePgTmgg5s9rs8MJR2LmDaReBvyn3Sv3aUnK1L7mHHexei9g-5tB-akfIGvXVsannni5Y68vnVfNLzfKVISVf0q-kLYthFE5BIX4pdVPAfTQlaIfxqsiqSmbsVaxbxmRS5Z6VV3FFUi4kpNl5b6p5nJPDAiBQLZw5m-KnpM6lTqWDFpJswiytg5NCwiHmAo6isa-AS5-ObYXgPEFTwInj8Xnux_EVdWwiQUXHzdFjtZfKjK2qxo6eaoV29RAdqSQIVyKUlrreT4tXh5jkr9HCmf9X9di5jfsko30mBGYKxXFOpchiMOakvm_mZ6-yOwuSKW083TaXN7yvUA_PiIabXDTHun1qsFgSi7WDqvLmzRti2T4LWE82gYJEXJdWmCw3B7Iw9ZXCiLH5N9ge0jp6Z9wKtk9cwG3h4Hzuc0qK59PU1SAph6a-eHmOkeOJ7tqBBH0Q02HKuQ84ix-SHgo_Mm3Xs_9tauZqeIGwyqSr8M6RDSQvcXhaQKM2wZXUznzq_kybvdhzdFRWjqayY3jzHoShTirBoEcPlhzGxcUlMQdfAH30U0DAFfV5-6bQpYP0dmi_Dtfv0LD5KLgUImr-QEpuutZXhGa5kXk0cs3uAGEv2nwgkL_fMooc-9pJxXTX7af61hBxJ9yVs66ag0lqpDspbt9ZgClMlKCGbx7HW9QzgpiP4IkZOtVLMGC3xG4_9h1sGEDNlmjw--L_ba_Eu1KEVO4-ze1FwXSagC8zbcFlPcsXDVlaaD6JvWE9-tBtd69moMfDhl2yzND3QtqwwT-zxGb5GPBr8ii7spW7JdMQfs9GTsw-iwa_b9lX0Mbm-flTOHSujYJH5ZjgPKScZNN8mEc83GFLpIFfbTYtTmdu00QMxmaXj8hP-Ydf08pNSjnp9ygAnKfIV7k4ZgEolfTnFcdoyZuREJRZneQMbF4jdLECtRl8SrtwfHUWZp1t542b40_OOrD9hJ857v01YRCtQ2vTDAYxAztDXMBFbOh2H_82MUU79VAk0kdBJ9Eh2H1JsoYmBOM6AZv8ENvWZRmJG1y863GvsTa--KqWVxZyAMNrK5edg06Ys9e9739kwmVvIgjjOZqG4-UmmmGBD9aRCbTvO2RYlxWg3aP8paeo4_3e5OVtW---h5JWzfFdyyXHgnLCIhMchYq5jUz4d00kNX-Pf4E_hj_mK2-g3e79gftVxSwPJd2yIl-TXF0Hfd1uDiMkc65bFM-RRL4GmBud489QOcVqNzpnTnVK78vQB-uMMxZik9HIHolvIT1m1Hg5pAhNcmsmN7yh9SwepUkdTRDVYzLWVHcpjw05-1sjpUg28Dw4P7_yzLf9-aJ5XwHfjr_02jrzDXFh13GFPAKUOPOUvktoJhOlRkySamg.n9KU0ACmveGQTOaCvhRVUw',
        'language': 'en',
        'loginType': '1',
        'ipAddress': '124.40.244.94',
        'latitude': '12.9716',
        'longitude': '77.5946',
        'platform': '0',
        'country': 'India',
        'zoneId': '5df8b7628798dc19d926bd29'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 200)
    
    def test_200_perfect_mendetory_data(self):
        ''' with mendetory field '''
        url = "https://api.shoppd.net/python/product/details?productId=61ea8b077e22d539eabf0bd4&cityId=5df7b7218798dc2c1114e6bf&parentProductId=61ea8b057e22d539eabf0bd2"

        payload={}
        headers = {
        'currencycode': 'INR',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.gByQGPBpSjq_02ouEMm1NPVi0ztFADiTZuPVquOsYBxbUnp1K29duTWG9UoxPDjaOmr4Y5xbJSlanyKzpolaFuJcj2nOD6yaOhaUttHLgDwxmZCDhKUn21IjxTPndI0Bq0bgNydrMom51pkr9HQbpvl4HwYeU_K4Fp8-hF_3nno.ooQk1k0R_zz_B05H.xiLEpsVzQnmsEhGz_dxAcubQYS_VSlGjXfn0cbPFvqEAftKf6kzXAXTPIgAUu6OmzTW_GC3V8jJA0iurGofo7lBxnnLO7lFQ5imd00riERSp54m5DpyaOP0z54ilTJNHPHioDkRmo7pRXMLmQ02twbIh75ndi4loYCwkU6ElDF6Vlw4MQhaFQEZAW5_Ki-vw4So5y-HVa6rGHnrFkZonNjEaYw1JuqynB6UfG_VMOJk-lEfnHFQCDP_TpTitB5LkIGj9-m2CuunAUhTbGmmvOk3QnlTv_L2Pb5Fv7s4pqPmMJNFVyLBNSUdVrIIBJBop9yxYqTIZi4oDnXXfS7rp4EDPbQYePgTmgg5s9rs8MJR2LmDaReBvyn3Sv3aUnK1L7mHHexei9g-5tB-akfIGvXVsannni5Y68vnVfNLzfKVISVf0q-kLYthFE5BIX4pdVPAfTQlaIfxqsiqSmbsVaxbxmRS5Z6VV3FFUi4kpNl5b6p5nJPDAiBQLZw5m-KnpM6lTqWDFpJswiytg5NCwiHmAo6isa-AS5-ObYXgPEFTwInj8Xnux_EVdWwiQUXHzdFjtZfKjK2qxo6eaoV29RAdqSQIVyKUlrreT4tXh5jkr9HCmf9X9di5jfsko30mBGYKxXFOpchiMOakvm_mZ6-yOwuSKW083TaXN7yvUA_PiIabXDTHun1qsFgSi7WDqvLmzRti2T4LWE82gYJEXJdWmCw3B7Iw9ZXCiLH5N9ge0jp6Z9wKtk9cwG3h4Hzuc0qK59PU1SAph6a-eHmOkeOJ7tqBBH0Q02HKuQ84ix-SHgo_Mm3Xs_9tauZqeIGwyqSr8M6RDSQvcXhaQKM2wZXUznzq_kybvdhzdFRWjqayY3jzHoShTirBoEcPlhzGxcUlMQdfAH30U0DAFfV5-6bQpYP0dmi_Dtfv0LD5KLgUImr-QEpuutZXhGa5kXk0cs3uAGEv2nwgkL_fMooc-9pJxXTX7af61hBxJ9yVs66ag0lqpDspbt9ZgClMlKCGbx7HW9QzgpiP4IkZOtVLMGC3xG4_9h1sGEDNlmjw--L_ba_Eu1KEVO4-ze1FwXSagC8zbcFlPcsXDVlaaD6JvWE9-tBtd69moMfDhl2yzND3QtqwwT-zxGb5GPBr8ii7spW7JdMQfs9GTsw-iwa_b9lX0Mbm-flTOHSujYJH5ZjgPKScZNN8mEc83GFLpIFfbTYtTmdu00QMxmaXj8hP-Ydf08pNSjnp9ygAnKfIV7k4ZgEolfTnFcdoyZuREJRZneQMbF4jdLECtRl8SrtwfHUWZp1t542b40_OOrD9hJ857v01YRCtQ2vTDAYxAztDXMBFbOh2H_82MUU79VAk0kdBJ9Eh2H1JsoYmBOM6AZv8ENvWZRmJG1y863GvsTa--KqWVxZyAMNrK5edg06Ys9e9739kwmVvIgjjOZqG4-UmmmGBD9aRCbTvO2RYlxWg3aP8paeo4_3e5OVtW---h5JWzfFdyyXHgnLCIhMchYq5jUz4d00kNX-Pf4E_hj_mK2-g3e79gftVxSwPJd2yIl-TXF0Hfd1uDiMkc65bFM-RRL4GmBud489QOcVqNzpnTnVK78vQB-uMMxZik9HIHolvIT1m1Hg5pAhNcmsmN7yh9SwepUkdTRDVYzLWVHcpjw05-1sjpUg28Dw4P7_yzLf9-aJ5XwHfjr_02jrzDXFh13GFPAKUOPOUvktoJhOlRkySamg.n9KU0ACmveGQTOaCvhRVUw',
        'language': 'en'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 200)

    def test_unauthorized_401_wrong_token_data(self):
        ''' userId must be in token without it token would not valide and you will get unathorizatrion error '''

        url = "https://api.shoppd.net/python/product/details?productId=61ea8b077e22d539eabf0bd4&cityId=5df7b7218798dc2c1114e6bf&parentProductId=61ea8b057e22d539eabf0bd2"

        payload={}
        headers = {
        'currencycode': 'INR',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.h4-g4_1LzV2Wh312iwc78WGejURMpurQNUC30oOIAg-73ZbkSZ4hhuu5wh3BumYz3O-5jWDCAFwkPa18kuByhB7eEgFV4sDSx0iI55GtgfMWR3QPMA-6ETZbwZ-RSQEw--yQSWEb75ZhMCyhFmc-wGmF_teqIJHkbXFmjYQeb6E.wufFxFu2pt3X2Ulh.EPTd8ZtAfoEhNWv_qZ2w6RUZKvZlmRcKg9SJggBJUmSHXQoI55V_t0nMRtHSAYrgaVyCf5QL1rAamDtaddQTwVC7STKp8Dsmfc6slTKHQgucBurKmpswkzNyA4pjKbUZC6-h5Kv6qgaAeZ6Q6lt2HnHzjLjAG-JGccl4_IoGtxnqgl_ICIsR1kYKmHS_2XFK-6s7UiFiflAqhBlpZOVkn-yisDDMZXdBrUbrtjbwKRiLoAYs5LmcH1LO9xoUkHKCDVAR0Zd3wqEfdRw1yT-99HkouXe_04YJSl6PgiZ1DxZ7cmVhZMt-tUriPgdWlb9QJyUw8oYnG9GC37RjjzkuML1GaeXonMJgMkqez9TFB3Xrah55dH7E639rOBuW5qaQr4fOL6kOxOwH3DFtGJB2uuxvOK08JU6pAwWgCo-Okd3oGXE5tNaZdCJtKeM28Cw65ZrYOWrKbNIGiwVUcXcBDQMg3gFYaPjoZYdF5GPu46mEA4pmPi9ea46BHLIkLUCHnqPpFZ9XgdR9QSKMhhXfsedUIjXsRx5BZUjKV6Ejpif8iot_C6U6F2woLPqEGhsAlXJAjFGx0RM-71agI9bTDi2RBXsK9f8sgx6-ClEelP7gbx3SAFLBlKjTGbFqA_0ndkClCkzMBSTfoHHP-Vb8sq6fh7a9-sKBq9y-qSCxcgig-Eblz53pC59-YFZl9trFcgKakQrunHz3Bl14shvB4qSB8Ww_mp2g_4g-VhA8_tYa-81Q9g5GjatyHdQBCuc6OEuoi9kAt555rDDieFDp1f-nU96V48adrx0eaPMMEJad1fI6VNavxq6UGxlrhMQgRbkse6u9P-dWebW02h6CPsO0ZkNm3z1ypLzOXJE6gVAly2pZk0lvapgRfGX8H-ryaEKEq60JmSr5oWhGp_-WtPjYeGNh0WQC9SP_5Jkz1CJDPCOnhJHcJLAWBdSE4LlFMmlTvFLshlp6VGdhPxqfUZQurKhFnVgl2wQkTtR0w21IZRNpOMZzsx3RtfEj94TxxG7sd13XHRsRUjApODV06yrhmlc5GzMnQKiOIyDVLz9cpIxqkLwsPSZOSaPOtLD4LEfyx-k2fJPYSSnObtslSQqd6-v1CUuImWq6WMf_sYff4T1GxzvTNdX03BE0k8Lc-p6LSNhZ7WTvDxM8ltE9OfFCFDvpaqhjLkY2M4matTENC66Kr-MIpWZvU3guM9TeJRIqWx9irklDX9MyYZSkacLns3LCY1RvhyPbImSf-DGHX4485aUku_wRQfL1X65IdugvHMqRQNnoZVkB-74BHIbG3jOZXtC7cxC0W2bCbW9GlkbHGZwe2klA_PW73gPkKXfW31Q6fSMR-r88DF4Eq27_UoQ1_HJCRG-R8iqAA6_7NqRv3_hIspoN6jJX5X5pxtaZ25KYV-v2_gqArs7ENRzlHqW-kEcwAmlgXrn9hQ-x12tI_qGHyToqghEP5KoRaFQ04cjfP737LmDEguJpW5GCKLHdB1XVFRg13MqdQSFHwCLYlfxFjSYtqZZd5HP7VmrqWyV3FfT-pxFQvMERHonQTgXI5UgKe6z8B55iRB7Q8OYAt2YyiJJmhm56fHzuhKbNVeh3Mug3nnvVvG1CZjQqmzC6-1LjZ3H7XVMTEUqwImdDAfIdhol4I7XL8O4GVnlGfYEDuy5fpYc6SOz8TlqBeNw77uuVDLkDtrkuZg76UdVNxjwvgfxWy_dDoUs89XI8AhXmYJS7VdLD0BNthBlewOasuVHfBRBN4-wCnxsptxw.TynZbAgHAWBQ9iaFHZxsaQ',
        'language': 'en'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 401)
    
    def test_unauthorized_401_data(self):
        ''' token field mendetory '''
        url = "https://api.shoppd.net/python/product/details?productId=61ea8b077e22d539eabf0bd4&cityId=5df7b7218798dc2c1114e6bf&parentProductId=61ea8b057e22d539eabf0bd2"

        payload={}
        headers = {
        'currencycode': 'INR',
        'Authorization': '',
        'language': 'en'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 400)

    def test_data_missing_product_id(self):
        ''' without send productId it will throw missing data error '''
        url = "https://api.shoppd.net/python/product/details?productId=&referralId=PRJC4&cityId=5df7b7218798dc2c1114e6bf&parentProductId=61ea8b057e22d539eabf0bd2&isSearch=1"
        payload={}
        headers = {
        'currencycode': 'INR',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.gByQGPBpSjq_02ouEMm1NPVi0ztFADiTZuPVquOsYBxbUnp1K29duTWG9UoxPDjaOmr4Y5xbJSlanyKzpolaFuJcj2nOD6yaOhaUttHLgDwxmZCDhKUn21IjxTPndI0Bq0bgNydrMom51pkr9HQbpvl4HwYeU_K4Fp8-hF_3nno.ooQk1k0R_zz_B05H.xiLEpsVzQnmsEhGz_dxAcubQYS_VSlGjXfn0cbPFvqEAftKf6kzXAXTPIgAUu6OmzTW_GC3V8jJA0iurGofo7lBxnnLO7lFQ5imd00riERSp54m5DpyaOP0z54ilTJNHPHioDkRmo7pRXMLmQ02twbIh75ndi4loYCwkU6ElDF6Vlw4MQhaFQEZAW5_Ki-vw4So5y-HVa6rGHnrFkZonNjEaYw1JuqynB6UfG_VMOJk-lEfnHFQCDP_TpTitB5LkIGj9-m2CuunAUhTbGmmvOk3QnlTv_L2Pb5Fv7s4pqPmMJNFVyLBNSUdVrIIBJBop9yxYqTIZi4oDnXXfS7rp4EDPbQYePgTmgg5s9rs8MJR2LmDaReBvyn3Sv3aUnK1L7mHHexei9g-5tB-akfIGvXVsannni5Y68vnVfNLzfKVISVf0q-kLYthFE5BIX4pdVPAfTQlaIfxqsiqSmbsVaxbxmRS5Z6VV3FFUi4kpNl5b6p5nJPDAiBQLZw5m-KnpM6lTqWDFpJswiytg5NCwiHmAo6isa-AS5-ObYXgPEFTwInj8Xnux_EVdWwiQUXHzdFjtZfKjK2qxo6eaoV29RAdqSQIVyKUlrreT4tXh5jkr9HCmf9X9di5jfsko30mBGYKxXFOpchiMOakvm_mZ6-yOwuSKW083TaXN7yvUA_PiIabXDTHun1qsFgSi7WDqvLmzRti2T4LWE82gYJEXJdWmCw3B7Iw9ZXCiLH5N9ge0jp6Z9wKtk9cwG3h4Hzuc0qK59PU1SAph6a-eHmOkeOJ7tqBBH0Q02HKuQ84ix-SHgo_Mm3Xs_9tauZqeIGwyqSr8M6RDSQvcXhaQKM2wZXUznzq_kybvdhzdFRWjqayY3jzHoShTirBoEcPlhzGxcUlMQdfAH30U0DAFfV5-6bQpYP0dmi_Dtfv0LD5KLgUImr-QEpuutZXhGa5kXk0cs3uAGEv2nwgkL_fMooc-9pJxXTX7af61hBxJ9yVs66ag0lqpDspbt9ZgClMlKCGbx7HW9QzgpiP4IkZOtVLMGC3xG4_9h1sGEDNlmjw--L_ba_Eu1KEVO4-ze1FwXSagC8zbcFlPcsXDVlaaD6JvWE9-tBtd69moMfDhl2yzND3QtqwwT-zxGb5GPBr8ii7spW7JdMQfs9GTsw-iwa_b9lX0Mbm-flTOHSujYJH5ZjgPKScZNN8mEc83GFLpIFfbTYtTmdu00QMxmaXj8hP-Ydf08pNSjnp9ygAnKfIV7k4ZgEolfTnFcdoyZuREJRZneQMbF4jdLECtRl8SrtwfHUWZp1t542b40_OOrD9hJ857v01YRCtQ2vTDAYxAztDXMBFbOh2H_82MUU79VAk0kdBJ9Eh2H1JsoYmBOM6AZv8ENvWZRmJG1y863GvsTa--KqWVxZyAMNrK5edg06Ys9e9739kwmVvIgjjOZqG4-UmmmGBD9aRCbTvO2RYlxWg3aP8paeo4_3e5OVtW---h5JWzfFdyyXHgnLCIhMchYq5jUz4d00kNX-Pf4E_hj_mK2-g3e79gftVxSwPJd2yIl-TXF0Hfd1uDiMkc65bFM-RRL4GmBud489QOcVqNzpnTnVK78vQB-uMMxZik9HIHolvIT1m1Hg5pAhNcmsmN7yh9SwepUkdTRDVYzLWVHcpjw05-1sjpUg28Dw4P7_yzLf9-aJ5XwHfjr_02jrzDXFh13GFPAKUOPOUvktoJhOlRkySamg.n9KU0ACmveGQTOaCvhRVUw',
        'language': 'en',
        'loginType': '1',
        'ipAddress': '124.40.244.94',
        'latitude': '12.9716',
        'longitude': '77.5946',
        'platform': '0',
        'country': 'India',
        'zoneId': '5df8b7628798dc19d926bd29'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 412)
    
    def test_data_missing_parent_product_id(self):
        ''' without send ParentproductId it will throw missing data error '''
        url = "https://api.shoppd.net/python/product/details?productId=61ea8b077e22d539eabf0bd4&referralId=PRJC4&cityId=5df7b7218798dc2c1114e6bf&parentProductId=&isSearch=1"

        payload={}
        headers = {
        'currencycode': 'INR',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.gByQGPBpSjq_02ouEMm1NPVi0ztFADiTZuPVquOsYBxbUnp1K29duTWG9UoxPDjaOmr4Y5xbJSlanyKzpolaFuJcj2nOD6yaOhaUttHLgDwxmZCDhKUn21IjxTPndI0Bq0bgNydrMom51pkr9HQbpvl4HwYeU_K4Fp8-hF_3nno.ooQk1k0R_zz_B05H.xiLEpsVzQnmsEhGz_dxAcubQYS_VSlGjXfn0cbPFvqEAftKf6kzXAXTPIgAUu6OmzTW_GC3V8jJA0iurGofo7lBxnnLO7lFQ5imd00riERSp54m5DpyaOP0z54ilTJNHPHioDkRmo7pRXMLmQ02twbIh75ndi4loYCwkU6ElDF6Vlw4MQhaFQEZAW5_Ki-vw4So5y-HVa6rGHnrFkZonNjEaYw1JuqynB6UfG_VMOJk-lEfnHFQCDP_TpTitB5LkIGj9-m2CuunAUhTbGmmvOk3QnlTv_L2Pb5Fv7s4pqPmMJNFVyLBNSUdVrIIBJBop9yxYqTIZi4oDnXXfS7rp4EDPbQYePgTmgg5s9rs8MJR2LmDaReBvyn3Sv3aUnK1L7mHHexei9g-5tB-akfIGvXVsannni5Y68vnVfNLzfKVISVf0q-kLYthFE5BIX4pdVPAfTQlaIfxqsiqSmbsVaxbxmRS5Z6VV3FFUi4kpNl5b6p5nJPDAiBQLZw5m-KnpM6lTqWDFpJswiytg5NCwiHmAo6isa-AS5-ObYXgPEFTwInj8Xnux_EVdWwiQUXHzdFjtZfKjK2qxo6eaoV29RAdqSQIVyKUlrreT4tXh5jkr9HCmf9X9di5jfsko30mBGYKxXFOpchiMOakvm_mZ6-yOwuSKW083TaXN7yvUA_PiIabXDTHun1qsFgSi7WDqvLmzRti2T4LWE82gYJEXJdWmCw3B7Iw9ZXCiLH5N9ge0jp6Z9wKtk9cwG3h4Hzuc0qK59PU1SAph6a-eHmOkeOJ7tqBBH0Q02HKuQ84ix-SHgo_Mm3Xs_9tauZqeIGwyqSr8M6RDSQvcXhaQKM2wZXUznzq_kybvdhzdFRWjqayY3jzHoShTirBoEcPlhzGxcUlMQdfAH30U0DAFfV5-6bQpYP0dmi_Dtfv0LD5KLgUImr-QEpuutZXhGa5kXk0cs3uAGEv2nwgkL_fMooc-9pJxXTX7af61hBxJ9yVs66ag0lqpDspbt9ZgClMlKCGbx7HW9QzgpiP4IkZOtVLMGC3xG4_9h1sGEDNlmjw--L_ba_Eu1KEVO4-ze1FwXSagC8zbcFlPcsXDVlaaD6JvWE9-tBtd69moMfDhl2yzND3QtqwwT-zxGb5GPBr8ii7spW7JdMQfs9GTsw-iwa_b9lX0Mbm-flTOHSujYJH5ZjgPKScZNN8mEc83GFLpIFfbTYtTmdu00QMxmaXj8hP-Ydf08pNSjnp9ygAnKfIV7k4ZgEolfTnFcdoyZuREJRZneQMbF4jdLECtRl8SrtwfHUWZp1t542b40_OOrD9hJ857v01YRCtQ2vTDAYxAztDXMBFbOh2H_82MUU79VAk0kdBJ9Eh2H1JsoYmBOM6AZv8ENvWZRmJG1y863GvsTa--KqWVxZyAMNrK5edg06Ys9e9739kwmVvIgjjOZqG4-UmmmGBD9aRCbTvO2RYlxWg3aP8paeo4_3e5OVtW---h5JWzfFdyyXHgnLCIhMchYq5jUz4d00kNX-Pf4E_hj_mK2-g3e79gftVxSwPJd2yIl-TXF0Hfd1uDiMkc65bFM-RRL4GmBud489QOcVqNzpnTnVK78vQB-uMMxZik9HIHolvIT1m1Hg5pAhNcmsmN7yh9SwepUkdTRDVYzLWVHcpjw05-1sjpUg28Dw4P7_yzLf9-aJ5XwHfjr_02jrzDXFh13GFPAKUOPOUvktoJhOlRkySamg.n9KU0ACmveGQTOaCvhRVUw',
        'language': 'en',
        'loginType': '1',
        'ipAddress': '124.40.244.94',
        'latitude': '12.9716',
        'longitude': '77.5946',
        'platform': '0',
        'country': 'India',
        'zoneId': '5df8b7628798dc19d926bd29'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 412)

    def test_500_error_data(self):
        ''' nee to send productId and ParentProductId  '''
        url = "https://api.shoppd.net/python/product/details?referralId=PRJC4&cityId=5df7b7218798dc2c1114e6bf&isSearch=1"

        payload={}
        headers = {
        'currencycode': 'INR',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.gByQGPBpSjq_02ouEMm1NPVi0ztFADiTZuPVquOsYBxbUnp1K29duTWG9UoxPDjaOmr4Y5xbJSlanyKzpolaFuJcj2nOD6yaOhaUttHLgDwxmZCDhKUn21IjxTPndI0Bq0bgNydrMom51pkr9HQbpvl4HwYeU_K4Fp8-hF_3nno.ooQk1k0R_zz_B05H.xiLEpsVzQnmsEhGz_dxAcubQYS_VSlGjXfn0cbPFvqEAftKf6kzXAXTPIgAUu6OmzTW_GC3V8jJA0iurGofo7lBxnnLO7lFQ5imd00riERSp54m5DpyaOP0z54ilTJNHPHioDkRmo7pRXMLmQ02twbIh75ndi4loYCwkU6ElDF6Vlw4MQhaFQEZAW5_Ki-vw4So5y-HVa6rGHnrFkZonNjEaYw1JuqynB6UfG_VMOJk-lEfnHFQCDP_TpTitB5LkIGj9-m2CuunAUhTbGmmvOk3QnlTv_L2Pb5Fv7s4pqPmMJNFVyLBNSUdVrIIBJBop9yxYqTIZi4oDnXXfS7rp4EDPbQYePgTmgg5s9rs8MJR2LmDaReBvyn3Sv3aUnK1L7mHHexei9g-5tB-akfIGvXVsannni5Y68vnVfNLzfKVISVf0q-kLYthFE5BIX4pdVPAfTQlaIfxqsiqSmbsVaxbxmRS5Z6VV3FFUi4kpNl5b6p5nJPDAiBQLZw5m-KnpM6lTqWDFpJswiytg5NCwiHmAo6isa-AS5-ObYXgPEFTwInj8Xnux_EVdWwiQUXHzdFjtZfKjK2qxo6eaoV29RAdqSQIVyKUlrreT4tXh5jkr9HCmf9X9di5jfsko30mBGYKxXFOpchiMOakvm_mZ6-yOwuSKW083TaXN7yvUA_PiIabXDTHun1qsFgSi7WDqvLmzRti2T4LWE82gYJEXJdWmCw3B7Iw9ZXCiLH5N9ge0jp6Z9wKtk9cwG3h4Hzuc0qK59PU1SAph6a-eHmOkeOJ7tqBBH0Q02HKuQ84ix-SHgo_Mm3Xs_9tauZqeIGwyqSr8M6RDSQvcXhaQKM2wZXUznzq_kybvdhzdFRWjqayY3jzHoShTirBoEcPlhzGxcUlMQdfAH30U0DAFfV5-6bQpYP0dmi_Dtfv0LD5KLgUImr-QEpuutZXhGa5kXk0cs3uAGEv2nwgkL_fMooc-9pJxXTX7af61hBxJ9yVs66ag0lqpDspbt9ZgClMlKCGbx7HW9QzgpiP4IkZOtVLMGC3xG4_9h1sGEDNlmjw--L_ba_Eu1KEVO4-ze1FwXSagC8zbcFlPcsXDVlaaD6JvWE9-tBtd69moMfDhl2yzND3QtqwwT-zxGb5GPBr8ii7spW7JdMQfs9GTsw-iwa_b9lX0Mbm-flTOHSujYJH5ZjgPKScZNN8mEc83GFLpIFfbTYtTmdu00QMxmaXj8hP-Ydf08pNSjnp9ygAnKfIV7k4ZgEolfTnFcdoyZuREJRZneQMbF4jdLECtRl8SrtwfHUWZp1t542b40_OOrD9hJ857v01YRCtQ2vTDAYxAztDXMBFbOh2H_82MUU79VAk0kdBJ9Eh2H1JsoYmBOM6AZv8ENvWZRmJG1y863GvsTa--KqWVxZyAMNrK5edg06Ys9e9739kwmVvIgjjOZqG4-UmmmGBD9aRCbTvO2RYlxWg3aP8paeo4_3e5OVtW---h5JWzfFdyyXHgnLCIhMchYq5jUz4d00kNX-Pf4E_hj_mK2-g3e79gftVxSwPJd2yIl-TXF0Hfd1uDiMkc65bFM-RRL4GmBud489QOcVqNzpnTnVK78vQB-uMMxZik9HIHolvIT1m1Hg5pAhNcmsmN7yh9SwepUkdTRDVYzLWVHcpjw05-1sjpUg28Dw4P7_yzLf9-aJ5XwHfjr_02jrzDXFh13GFPAKUOPOUvktoJhOlRkySamg.n9KU0ACmveGQTOaCvhRVUw',
        'language': 'en',
        'loginType': '1',
        'ipAddress': '124.40.244.94',
        'latitude': '12.9716',
        'longitude': '77.5946',
        'platform': '0',
        'country': 'India',
        'zoneId': '5df8b7628798dc19d926bd29'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 500)

    def test_500_error_wrong_case_data(self):
        ''' All params value must be camel case otherwise you get a missing field erro '''
        url = "https://api.shoppd.net/python/product/details?productid=61ea8b077e22d539eabf0bd4&cityid=5df7b7218798dc2c1114e6bf&parentproductid=61ea8b057e22d539eabf0bd2"

        payload={}
        headers = {
        'currencycode': 'INR',
        'Authorization': 'Bearer eyJhbGciOiJSU0EtT0FFUCIsImN0eSI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJ0eXAiOiJKV1QifQ.gByQGPBpSjq_02ouEMm1NPVi0ztFADiTZuPVquOsYBxbUnp1K29duTWG9UoxPDjaOmr4Y5xbJSlanyKzpolaFuJcj2nOD6yaOhaUttHLgDwxmZCDhKUn21IjxTPndI0Bq0bgNydrMom51pkr9HQbpvl4HwYeU_K4Fp8-hF_3nno.ooQk1k0R_zz_B05H.xiLEpsVzQnmsEhGz_dxAcubQYS_VSlGjXfn0cbPFvqEAftKf6kzXAXTPIgAUu6OmzTW_GC3V8jJA0iurGofo7lBxnnLO7lFQ5imd00riERSp54m5DpyaOP0z54ilTJNHPHioDkRmo7pRXMLmQ02twbIh75ndi4loYCwkU6ElDF6Vlw4MQhaFQEZAW5_Ki-vw4So5y-HVa6rGHnrFkZonNjEaYw1JuqynB6UfG_VMOJk-lEfnHFQCDP_TpTitB5LkIGj9-m2CuunAUhTbGmmvOk3QnlTv_L2Pb5Fv7s4pqPmMJNFVyLBNSUdVrIIBJBop9yxYqTIZi4oDnXXfS7rp4EDPbQYePgTmgg5s9rs8MJR2LmDaReBvyn3Sv3aUnK1L7mHHexei9g-5tB-akfIGvXVsannni5Y68vnVfNLzfKVISVf0q-kLYthFE5BIX4pdVPAfTQlaIfxqsiqSmbsVaxbxmRS5Z6VV3FFUi4kpNl5b6p5nJPDAiBQLZw5m-KnpM6lTqWDFpJswiytg5NCwiHmAo6isa-AS5-ObYXgPEFTwInj8Xnux_EVdWwiQUXHzdFjtZfKjK2qxo6eaoV29RAdqSQIVyKUlrreT4tXh5jkr9HCmf9X9di5jfsko30mBGYKxXFOpchiMOakvm_mZ6-yOwuSKW083TaXN7yvUA_PiIabXDTHun1qsFgSi7WDqvLmzRti2T4LWE82gYJEXJdWmCw3B7Iw9ZXCiLH5N9ge0jp6Z9wKtk9cwG3h4Hzuc0qK59PU1SAph6a-eHmOkeOJ7tqBBH0Q02HKuQ84ix-SHgo_Mm3Xs_9tauZqeIGwyqSr8M6RDSQvcXhaQKM2wZXUznzq_kybvdhzdFRWjqayY3jzHoShTirBoEcPlhzGxcUlMQdfAH30U0DAFfV5-6bQpYP0dmi_Dtfv0LD5KLgUImr-QEpuutZXhGa5kXk0cs3uAGEv2nwgkL_fMooc-9pJxXTX7af61hBxJ9yVs66ag0lqpDspbt9ZgClMlKCGbx7HW9QzgpiP4IkZOtVLMGC3xG4_9h1sGEDNlmjw--L_ba_Eu1KEVO4-ze1FwXSagC8zbcFlPcsXDVlaaD6JvWE9-tBtd69moMfDhl2yzND3QtqwwT-zxGb5GPBr8ii7spW7JdMQfs9GTsw-iwa_b9lX0Mbm-flTOHSujYJH5ZjgPKScZNN8mEc83GFLpIFfbTYtTmdu00QMxmaXj8hP-Ydf08pNSjnp9ygAnKfIV7k4ZgEolfTnFcdoyZuREJRZneQMbF4jdLECtRl8SrtwfHUWZp1t542b40_OOrD9hJ857v01YRCtQ2vTDAYxAztDXMBFbOh2H_82MUU79VAk0kdBJ9Eh2H1JsoYmBOM6AZv8ENvWZRmJG1y863GvsTa--KqWVxZyAMNrK5edg06Ys9e9739kwmVvIgjjOZqG4-UmmmGBD9aRCbTvO2RYlxWg3aP8paeo4_3e5OVtW---h5JWzfFdyyXHgnLCIhMchYq5jUz4d00kNX-Pf4E_hj_mK2-g3e79gftVxSwPJd2yIl-TXF0Hfd1uDiMkc65bFM-RRL4GmBud489QOcVqNzpnTnVK78vQB-uMMxZik9HIHolvIT1m1Hg5pAhNcmsmN7yh9SwepUkdTRDVYzLWVHcpjw05-1sjpUg28Dw4P7_yzLf9-aJ5XwHfjr_02jrzDXFh13GFPAKUOPOUvktoJhOlRkySamg.n9KU0ACmveGQTOaCvhRVUw',
        'language': 'en'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        self.assertEqual(response.status_code, 500)
        
