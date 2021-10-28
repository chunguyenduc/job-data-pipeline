# from django.db import models
from djongo import models
from django import forms
# Create your models here.


class Skill(models.Model):
    name = models.CharField(max_length=100)

    class Meta:
        abstract = True
class SkillModel(Skill):
    pass


class SkillForm(forms.ModelForm):
    class Meta:
        model = Skill
        fields = ('name', )



class Job(models.Model):
    title = models.CharField(max_length=150)
    city = models.CharField(max_length=100)
    skills = models.ArrayField(
        model_container=Skill,
        model_form_class=SkillForm
    )
