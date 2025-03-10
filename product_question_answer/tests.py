from distutils.command.clean import clean
from email import header
from urllib.parse import ParseResultBytes
from django.test import TestCase
from django.test import Client
from httpx import post
# Create your tests here.

client = Client()


# POST product question API ---
class TestProductQuestion(TestCase):
    ''' ProductQuestion '''
    # get API -------------------------------------
    
    def test_data_missing_productquestion_get(self):
        url = "/productQuestion"
        param = {
        }
        response = self.client.get(
            url, param,  content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_missing_productquestiuon_get(self):
        url = '/productQuestion'
        data = {
            "skip": 0
        }
        response = self.client.post(
            url, data, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_add_productquestion_get(self):
        url = "/productQuestion"
        param = {
            "skip": 0,
            "limit": 20
        }
        response = self.client.get(
            url, param,  content_type="application/json")
        self.assertEqual(response.status_code, 422)

    # post API-----------------------------------
    def test_data_missing_productquestion(self):
        url = "/productQuestion"
        data = {
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_missing_productquestiuon(self):
        url = '/productQuestion'
        data = {
            "productId": "61fba2c911d600fed35439fd",
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_empty_productquestiuon(self):
        url = '/productQuestion'
        data = {
            "productId": "",
            "question": "can you provide us your store number?"
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_wrong_productquestiuon(self):
        url = '/productQuestion'
        data = {
            "productID": "",
            "question": "can you provide us your store number?"
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_token_missing_productquestiuon(self):
        url = '/productQuestion'
        data = {
            "productId": "",
            "question": "can you provide us your store number?"
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='', content_type="application/json")
        self.assertEqual(response.status_code, 401)

    def test_token_empty_productquestiuon(self):
        url = '/productQuestion'
        data = {
            "productId": "61fba2c911d600fed35439fd",
            "question": "can we know your store address?"
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": ""}', content_type="application/json")
        self.assertEqual(response.status_code, 200)

    def test_add_productquestion(self):
        url = "/productQuestion"
        body = {
            "productId": "61fba2c911d600fed35439fd",
            "question": "Can we get live demo of your product?"
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 200)

    # patch API ------------------------------------------------------
    def test_data_missing_productquestion_patch(self):
        url = "/productQuestion"
        data = {
        }
        response = self.client.patch(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_missing_productquestiuon_patch(self):
        url = '/productQuestion'
        data = {
            "questionId": "61fba2c911d600fed35439fd",
        }
        response = self.client.patch(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_empty_productquestiuon_patch(self):
        url = '/productQuestion'
        data = {
            "questionId": "",
            "status": 1
        }
        response = self.client.patch(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_wrong_productquestiuon_patch(self):
        url = '/productQuestion'
        data = {
            "questionId": "61fa0f0f261015cf0a74c5d0",
            "status": 5
        }
        response = self.client.patch(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_token_missing_productquestiuon_patch(self):
        url = '/productQuestion'
        data = {
            "questionId": "61fa0f0f261015cf0a74c5d0",
            "status": 3
        }
        response = self.client.patch(
            url, data, HTTP_AUTHORIZATION='', content_type="application/json")
        self.assertEqual(response.status_code, 401)

    def test_add_productquestion_patch(self):
        url = "/productQuestion"
        body = {
            "questionId": "61fba2c911d600fed35439fd",
            "status": 3
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 200)


class TestproductQuestionAnswer(TestCase):
    # get API----------------------------
    def test_data_missing_productquestion_answer_get(self):
        url = "/productQuestionAnswer"
        param = {
        }
        response = self.client.get(
            url, param,  content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_missing_productquestiuon_answer_get(self):
        url = '/productQuestionAnswer'
        param = {
            "skip":0
        }
        response = self.client.get(
            url, param,  content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_add_productquestion_answer_get(self):
        url = "/productQuestionAnswer"
        param = {
            "skip": 0,
            "limit": 20,
            "questionId": "61fa12c66214d421928aa729"
        }
        response = self.client.get(
            url, param,  content_type="application/json")
        self.assertEqual(response.status_code, 422)

    # Post API------------------------
    def test_data_missing_productquestionAnswer(self):
        url = "/productQuestionAnswer"
        data = {
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_missing_productquestiuonAnswer(self):
        url = '/productQuestionAnswer'
        data = {

            "questionId": "62011fc3eb2074511fdca039",
            "postedAsAnonymous": True
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_empty_productquestiuonAnswer(self):
        url = '/productQuestionAnswer'
        data = {

            "answer": "no mam",
            "questionId": "62011fc3eb2074511fdca039",
            "postedAsAnonymous": ""
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_wrong_productquestiuonAnswer(self):
        url = '/productQuestionAnswer'
        data = {
            "answer": "no mam",
            "questionId": "62011fc3eb2074511fdca039",
            "postedAsAnonymous": "5411"
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 400)

    # def test_token_missing_productquestiuonAnswer(self):
    #     url = '/productQuestionAnswer'
    #     data = {
    #         {
    #             "answer": "no mam",
    #             "questionId": "62011fc3eb2074511fdca039",
    #             "postedAsAnonymous": False
    #         }
    #     }
    #     response = self.client.post(
    #         url, data, HTTP_AUTHORIZATION='', content_type="application/json")
        self.assertEqual(response.status_code, 401)

    def test_token_empty_productquestiuonAnswer(self):
        url = '/productQuestionAnswer'
        data = {
            "productId": "61fba2c911d600fed35439fd",
            "question": "can we know your store address?"
        }
        response = self.client.post(
            url, data, HTTP_AUTHORIZATION='{"userId": ""}', content_type="application/json")
        self.assertEqual(response.status_code, 200)

    def test_add_productquestionAnswer(self):
        url = "/productQuestionAnswer"
        body = {

            "answer": "no mam",
            "questionId": "62011fc3eb2074511fdca039",
            "postedAsAnonymous": True
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 200)

    # patch API-------------------------------------
    def test_data_missing_productquestionAnswer_patch(self):
        url = "/productQuestionAnswer"
        data = {
        }
        response = self.client.patch(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_missing_productquestiuonAnswer_patch(self):
        url = '/productQuestionAnswer'
        data = {
            "questionId": "61fba2c911d600fed35439fd",
        }
        response = self.client.patch(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_empty_productquestiuonAnswer_patch(self):
        url = '/productQuestionAnswer'
        data = {
            "questionId": "",
            "status": 1
        }
        response = self.client.patch(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_datakey_wrong_productquestiuonAnswer_patch(self):
        url = '/productQuestionAnswer'
        data = {
            "questionId": "61fa0f0f261015cf0a74c5d0",
            "status": 5
        }
        response = self.client.patch(
            url, data, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_productQuestionAnswer_patch(self):
        url = "/productQuestionAnswer"
        body = {
            "questionId": "61fba2c911d600fed35439fd",
            "status": 3
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 200)


class TestReportProductQuestionAnswer(TestCase):
    # get API-----------------------------
    def test_data_missing_report_get(self):
        url = "/reportProductQuestionAnswer"
        param = {
        }
        response = self.client.get(
            url, param,  content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_value_wrong_get(self):
        url = "/reportProductQuestionAnswer"
        param = {
             "answerId": "62038f3dc9e07037cb012800"
        }
        response = self.client.get(
            url, param,  content_type="application/json")
        self.assertEqual(response.status_code, 404)

    def test_data_empty_value_report_get(self):
        url = "/reportProductQuestionAnswer"
        params = {
            "answerId": ""
        }
        response = self.client.get(
            url, params)
        self.assertEqual(response.status_code, 404)

    def test_data_report_get(self):
        url = "/reportProductQuestionAnswer"
        params = {
            "answerId": "62038f3dc9e07037cb01283b"
        }
        response = self.client.get(
            url, params)
        self.assertEqual(response.status_code, 200)

    # post API------------------------
    def test_data_missing_report_productanswer(self):
        url = "/reportProductQuestionAnswer"
        body = {
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION='{"userId": "5ead7eb2985fdd515e2fae4a"}', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_empty_report_productanswer(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": ""
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_report_productanswer(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011ff0eb2074511fdca000"
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 404)

    def test_data_wrongkey_report_productanswer(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerID": "62011ff0eb2074511fdcaccc"
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_empty_token_report_productanswer(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011ff0eb2074511fdcaccc"
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": ""}, content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def test_not_token_report_productanswer(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011ff0eb2074511fdcaccc"
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION='', content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def test_add_report_anser(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011ff0eb2074511fdcaccc"
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 200)


class TestProductQuestionAnswerLike(TestCase):
    # get API--------------------------------------
    def test_data_missing_answer_like_get(self):
        url = "/reportProductQuestionAnswer"
        body = {
        }
        response = self.client.get(
            url, body)
        self.assertEqual(response.status_code, 422)

    def test_data_missing_answer_like_get(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": ""
        }
        response = self.client.get(
            url, body)
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_answer_like_get(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62038f3dc9e07037cb012800"
        }
        response = self.client.get(
            url, body)
        self.assertEqual(response.status_code, 404)

    def test_data_missing_answer_like_get(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62038f3dc9e07037cb01283b"
        }
        response = self.client.get(
            url, body)
        self.assertEqual(response.status_code, 200)

    # post API---------------------------------------
    def test_data_missing_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {

        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def testa_data_empty_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "",
            "vote": 0
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 450
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_key_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerID": "62011fdeeb2074511fdca03a",
            "vote": 0
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_empty_token_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 0
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION='', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_empty_token_value_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 0
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": ""}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_add_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 0
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    # dislike
    def test_data_missing_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {

        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def testa_data_empty_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "",
            "vote": 1
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 450
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_key_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerID": "62011fdeeb2074511fdca03a",
            "vote": 1
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_empty_token_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 1
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION='', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_empty_token_value_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 1
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": ""}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_add_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 1
        }
        response = self.client.post(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 200)

    # patch API---------------------------------------------------------------------------------
    def test_data_missing_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {

        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def testa_data_empty_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "",
            "vote": 0
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 450
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_key_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerID": "62011fdeeb2074511fdca03a",
            "vote": 0
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_empty_token_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 0
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION='', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_empty_token_value_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 0
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": ""}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_add_answer_like(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 0
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    # dislike
    def test_data_missing_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {

        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def testa_data_empty_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "",
            "vote": 1
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 450
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_data_wrong_key_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerID": "62011fdeeb2074511fdca03a",
            "vote": 1
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_empty_token_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 1
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION='', content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_empty_token_value_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 1
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": ""}, content_type="application/json")
        self.assertEqual(response.status_code, 422)

    def test_add_answer_dislike(self):
        url = "/reportProductQuestionAnswer"
        body = {
            "answerId": "62011fdeeb2074511fdca03a",
            "vote": 1
        }
        response = self.client.patch(
            url, body, HTTP_AUTHORIZATION={"userId": "5ead7eb2985fdd515e2fae4a"}, content_type="application/json")
        self.assertEqual(response.status_code, 200)
