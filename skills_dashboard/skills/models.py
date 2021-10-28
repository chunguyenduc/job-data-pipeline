# from django.db import models
import django
from djongo import models
# from django import forms
# Create your models here.


class Skill(models.Model):
    name = models.CharField(max_length=100, unique=True)

    class Meta:
        abstract = True
class SkillModel(Skill):
    pass

    class Meta:
        # fields = ('id', 'name')
        db_table = 'skills_skill'



class Job(models.Model):
    title = models.CharField(max_length=150)
    city = models.CharField(max_length=100)
    skills = models.ArrayField(
        model_container=Skill,
    )
    class Meta:
        db_table = 'jobs'

