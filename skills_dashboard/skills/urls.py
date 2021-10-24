from django.urls import path
from rest_framework.urlpatterns import format_suffix_patterns
from skills import views

urlpatterns = [
    path('skills/', views.SkillList.as_view()),
    path('skills/<int:pk>/', views.SkillDetail.as_view()),
]

# urlpatterns = format_suffix_patterns(urlpatterns)

