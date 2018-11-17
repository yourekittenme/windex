from django.db import models
from django.urls import reverse


class Stock(models.Model):
    """A single publicly traded stock"""
    symbol = models.CharField(max_length=20)
    company = models.CharField(max_length=100, blank=True)
    shares_outstanding = models.IntegerField()
    current_price = models.DecimalField(decimal_places=2, max_digits=10)
    prior_close_price = models.DecimalField(decimal_places=2, max_digits=10)
    change_price = models.DecimalField(decimal_places=2, max_digits=10)
    high_price = models.DecimalField(decimal_places=2, max_digits=10)
    low_price = models.DecimalField(decimal_places=2, max_digits=10)
    high_price_52_weeks = models.DecimalField(decimal_places=2, max_digits=10)
    low_price_52_weeks = models.DecimalField(decimal_places=2, max_digits=10)
    market_cap = models.DecimalField(decimal_places=2, max_digits=20)
    logo = models.ImageField(upload_to='')
    inactive = models.BooleanField(default=False)

    def __str__(self):
        return self.symbol


class Observations(models.Model):
    """Daily statistics about how a stock is trading"""
    stock_id = models.ForeignKey('Stock', on_delete=models.CASCADE)
    observation_date = models.DateTimeField()
    shares_outstanding = models.IntegerField()
    volume = models.IntegerField()
    open_price = models.DecimalField(decimal_places=2, max_digits=10)
    high_price = models.DecimalField(decimal_places=2, max_digits=10)
    low_price = models.DecimalField(decimal_places=2, max_digits=10)
    close_price = models.DecimalField(decimal_places=2, max_digits=10)


class Index(models.Model):
    """An indicator of stock market performance based on information about selected stocks"""
    name = models.CharField(max_length=100)
    short_name = models.CharField(max_length=10)
    current_value = models.DecimalField(decimal_places=2, max_digits=10)
    prior_close_value = models.DecimalField(decimal_places=2, max_digits=10)
    change_value = models.DecimalField(decimal_places=2, max_digits=10)
    high_value = models.DecimalField(decimal_places=2, max_digits=10)
    low_value = models.DecimalField(decimal_places=2, max_digits=10)
    inactive = models.BooleanField(default=False)

    def __str__(self):
        return ''.join([self.short_name, ' ', self.name])


class StocksIndexed(models.Model):
    """Describes which stocks belong to an index"""
    index_id = models.ForeignKey('Index', on_delete=models.CASCADE)
    stock_id = models.ForeignKey('Stock', on_delete=models.CASCADE)
