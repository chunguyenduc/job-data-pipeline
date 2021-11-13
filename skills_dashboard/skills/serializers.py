from rest_framework import serializers
from skills.models import Skill, Job, SkillModel
from bson.objectid import ObjectId
from bson.errors import InvalidId
from django.utils.encoding import smart_text
...
class ObjectIdField(serializers.Field):
    """ Serializer field for Djongo ObjectID fields """
    def to_internal_value(self, data):
        # Serialized value -> Database value
        try:
            return ObjectId(str(data))  # Get the ID, then build an ObjectID instance using it
        except InvalidId:
            raise serializers.ValidationError(
                '{} is not a valid ObjectID'.format(data))

    def to_representation(self, value):
        # Database value -> Serialized value
        if not ObjectId.is_valid(value):  # User submitted ID's might not be properly structured
            raise InvalidId
        return smart_text(value)

class SkillSerializer(serializers.ModelSerializer):
    _id = ObjectIdField(read_only=True)
    class Meta:
        model = SkillModel

        fields = ('_id', 'name', )
        # fields = ('id', 'name')

class JobSerializer(serializers.ModelSerializer):
    _id = ObjectIdField(read_only=True)
    from_date = serializers.DateField(required=False)
    to_date = serializers.DateField(required=False)
    # total_job_by_city = serializers.
    class Meta:
        model = Job
        fields = ('_id', 'title', 'city', 'skills', 'company', 'url', 'site', 'created_at', 'from_date', 'to_date')

class JobListQuerySerializer(serializers.Serializer):
    title = serializers.CharField(max_length=150, required=False)
    city = serializers.CharField(max_length=100, required=False)
    company = serializers.CharField(max_length=100, required=False)
    site = serializers.CharField(max_length=100, required=False)
    from_date = serializers.DateField(required=False)
    to_date = serializers.DateField(required=False)


# class JobListSerializerByDate(serializers.ModelSerializer):
"""
{
  "statistic": [
    {
      "date": {
        "year": 2021,
        "month": 11,
        "day": 7
      }
    },
    {
      "date": {
        "year": 2021,
        "month": 11,
        "day": 8
      },
      "transactions": 8,
      "userChargeAmount": "2276000",
      "amount": "2276000"
    },
    {
      "date": {
        "year": 2021,
        "month": 11,
        "day": 9
      },
      "transactions": 2,
      "userChargeAmount": "589000",
      "amount": "589000"
    },
    {
      "date": {
        "year": 2021,
        "month": 11,
        "day": 10
      },
      "transactions": 1,
      "userChargeAmount": "241500",
      "amount": "241500"
    },
    {
      "date": {
        "year": 2021,
        "month": 11,
        "day": 11
      },
      "transactions": 2,
      "userChargeAmount": "482000",
      "amount": "482000"
    },
    {
      "date": {
        "year": 2021,
        "month": 11,
        "day": 12
      },
      "transactions": 1,
      "userChargeAmount": "241500",
      "amount": "241500"
    },
    {
      "date": {
        "year": 2021,
        "month": 11,
        "day": 13
      }
    }
  ],
  "totalTransactions": 14,
  "totalAmount": "14",
  "totalUserChargeAmount": "3830000",
  "totalDiscountAmount": "3830000"
}
"""
