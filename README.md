# External Data Downloader 

> This is a simple python script to download data from external sources.
> It is intended to be used as a submodule in other projects.

## Usage


Default options are set in the config file.
To Download data from the datasource required credentials are needed. 
Change the config file to your needs.

>Activate virtual environment for the python 

<details>
<summary>How to Install</summary>
<hr>

```python3 -m venv venv```

```source venv/bin/activate```

```pip install --upgrade pip```

```pip install -r requirements.txt```


<hr>
</details>

> ### Default:The script will search one day before for the IGU data 

``` python main.py``` 

> ### Options: two days before

``` python main.py -d 2```

> ### Options: to download the bulletin a 

``` python main.py -a ```

> ### Options: to download the bulletin ```a```, ```b```, ```c```, ```d``` and download the igu data belonging two days before

``` python main.py -d 2 -abcDIR```
> ### Options: to download ranged data from 2 to 10 days before range must be separated by a colon

``` python main.py -d 2:10``` 

> Note: The script will download the data in the designated folder with the date as the folder name (_i.e.,_ YYYMMDD).