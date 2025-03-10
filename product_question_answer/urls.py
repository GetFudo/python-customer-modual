from unicodedata import name
from django.conf.urls import url
from product_question_answer import views

app_name = 'product_question_answer'

urlpatterns = [
    url(r'^productQuestion$', views.ProductQuestion.as_view(), name="productQuestions"),
    url(r'^productQuestionAnswer$', views.ProductQuestionAnswer.as_view(), name="productQuestionAnswer"),
    url(r'^productQuestionAnswerLike$', views.ProductQuestionAnswerLike.as_view(), name="productQuestionAnswer"),
    url(r'^reportProductQuestionAnswer$' , views.ReportProductQuestionAnswer.as_view(), name="reportProductQuestionAnswer"),
    url(r'^adminProductQuestion$' , views.AdminProductQuestion.as_view(), name="adminProductQuestion"),
    url(r'^userLikecount$' , views.UserLikeCount.as_view(), name="userLikecount"),
    url(r'^userDislikeCount$' , views.UserDislikeCount.as_view(), name="userDislikeCount"),
    url(r'^userReportCount$' , views.UserReportCount.as_view(), name="userReportCount")
]
