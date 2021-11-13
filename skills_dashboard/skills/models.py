from django.db import models
from django.contrib.postgres.fields import ArrayField
# from djongo.models.fields import JSONField
# from djongo.models import ArrayField
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
    title = models.CharField(max_length=150, null=False, blank=False)
    city = models.CharField(max_length=100)
    skills = ArrayField(models.CharField(max_length=50))
    company = models.CharField(max_length=100)
    url = models.URLField(max_length=200)
    site = models.CharField(max_length=100, default=None)
    created_at = models.DateTimeField(null=True, blank=True)
    class Meta:
        db_table = 'jobs'
    def __str__(self):
        return self.title

