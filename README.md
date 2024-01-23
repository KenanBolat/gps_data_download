# External Data Downloader 

> This is a simple python script to download data from external sources.
> It is intended to be used as a submodule in other projects.

## Usage


Default options are set in the config file.
To Download data from the datasource required credentials are needed. 
Change the config file to your needs.

>Activate virtual environment for the python 

<hr>

```python3 -m venv venv```

```source venv/bin/activate```

```pip install --upgrade pip```

```pip install -r requirements.txt```


<hr>

> ### Default:The script will search one day before 

``` python main.py``` 

> ### Options: two days before

``` python main.py -d 2```

> ### Options: to download the bulletin a 

``` python main.py -a ```

> ### Options: to download the bulletin a and download the data belonging two days before

``` python main.py -a -d 2```