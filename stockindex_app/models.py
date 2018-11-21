from django.db import models
from django.urls import reverse


class Stock(models.Model):
    """A single publicly traded stock"""
    market = models.CharField(max_length=10)
    symbol = models.CharField(max_length=20)
    marketsymbol = models.CharField(max_length=32)
    company = models.CharField(max_length=100, blank=True)
    shares_outstanding = models.IntegerField(null=True)
    current_price = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    prior_close_price = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    change_price = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    high_price = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    low_price = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    high_price_52_weeks = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    low_price_52_weeks = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    market_cap = models.DecimalField(decimal_places=2, max_digits=20, null=True)
    logo = models.ImageField(upload_to='', null=True)
    inactive = models.BooleanField(default=False)

    def __str__(self):
        return self.symbol


class Observations(models.Model):
    """Daily statistics about how a stock is trading"""
    stock = models.ForeignKey('Stock', on_delete=models.CASCADE)
    observation_date = models.DateTimeField()
    volume = models.IntegerField(null=True)
    open_price = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    high_price = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    low_price = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    close_price = models.DecimalField(decimal_places=2, max_digits=10, null=True)


class Index(models.Model):
    """An indicator of stock market performance based on information about selected stocks"""
    name = models.CharField(max_length=100)
    short_name = models.CharField(max_length=10)
    current_value = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    prior_close_value = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    change_value = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    high_value = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    low_value = models.DecimalField(decimal_places=2, max_digits=10, null=True)
    inactive = models.BooleanField(default=False)

    def __str__(self):
        return ''.join([self.short_name, ' ', self.name])


class StocksIndexed(models.Model):
    """Describes which stocks belong to an index"""
    index = models.ForeignKey('Index', on_delete=models.CASCADE)
    stock = models.ForeignKey('Stock', on_delete=models.CASCADE)
