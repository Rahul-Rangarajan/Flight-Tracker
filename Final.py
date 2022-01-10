#!/usr/bin/env python
# coding: utf-8

# In[10]:


from pyspark.sql import SparkSession
spark = SparkSession.builder     .appName("test")    .getOrCreate()


# In[11]:


spark.stop()


# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as func
spark = SparkSession.builder     .appName("Final Project")    .getOrCreate()


# In[2]:


#Set user inputs
airport_df=spark.read.option("header",True)     .csv("airports.csv")

userLat=0
userLon=0

zoom=2
userRad=360/2**(zoom-2)

#convert to Unix time
userStart=1627905610
userEnd=1627948790

userAlt=100000

field_elevation=(672*.305)


# In[3]:


#reduce our DataFrame to the stuff we need
full_df=spark.read.option("header",False)     .csv("24hours.csv")
full_df = full_df.withColumnRenamed("_c0","Time")
full_df = full_df.withColumnRenamed("_c1","icao24")
full_df = full_df.withColumnRenamed("_c2","Lat")
full_df = full_df.withColumn("Lat",full_df.Lat.cast("float")-userLat)
full_df = full_df.withColumnRenamed("_c3","Lon")
full_df = full_df.withColumn("Lon",full_df.Lon.cast("float")-userLon)
full_df = full_df.withColumnRenamed("_c7","Callsign")
full_df = full_df.withColumnRenamed("_c8","OnGround")
full_df = full_df.withColumnRenamed("_c12","Alt")
full_df = full_df.withColumn("Alt",full_df.Alt.cast("float"))
reduce_df = full_df.drop("_c4","_c5","_c6","_c9","_c10","_c11","_c13","_c14","_c15")
#print(reduce_df.take(10))

# In[4]:

#create a fully filtered DataFrame
filtered_df=reduce_df.filter(reduce_df.Time <= userEnd)
filtered_df=filtered_df.filter(filtered_df.Time >= userStart)
#print(filtered_df.show(10,vertical=True))

filtered_df=filtered_df.filter(func.abs((filtered_df.Lat)) < userRad)
filtered_df=filtered_df.filter(func.abs((filtered_df.Lon)) < userRad)
#print(filtered_df.show(10, vertical=True))

filtered_df=filtered_df.filter(filtered_df.Alt<=userAlt)
#print(filtered_df.show(10,vertical=True))


# In[5]:


#download the picture
import requests
key="CtnQt4zaWXTKzTqZRIeR97svfaDFObRb"
print("Getting Map...")
param={"center":str(userLat)+","+str(userLon),"key":key,"zoom":zoom,"dim":0,"size":"1024,1024"}
req=requests.get("https://open.mapquestapi.com/staticmap/v4/getmap",params=param)
with open("map.jpg","wb") as image:
    image.write(req.content)

# In[6]:

#sorting dataframe for plotting
filtered_df = filtered_df.sort(filtered_df.Time)
#print(filtered_df.show(10,vertical=True))
sorted_df = filtered_df.groupBy("icao24").agg(func.collect_list("Lat"),func.collect_list("Lon"),func.collect_list("Alt"))
#print(sorted_df.show(10,vertical=True))

# In[ ]:

#create the plot
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.cbook import get_sample_data
import matplotlib.image as mpimg
import numpy as np
import csv
import PIL
from PIL import Image

print("Opening Map...")
im = Image.open("map.jpg")
print("Converting to PNG and flipping...")
im = im.transpose(PIL.Image.FLIP_TOP_BOTTOM)
im.save("flipped_map.png")
img=mpimg.imread('flipped_map.png')
#print("imgtype",type(img))

#create meshgrid the size of the image
print("Creating Grid...")
x, y = np.meshgrid(np.arange(-userRad/2,userRad/2,userRad/1024),np.arange(-userRad/2,userRad/2,userRad/1024))

print("Creating 3D plot...")
fig = plt.figure()
ax = plt.axes(projection='3d')

print("Adding map to plot...")
ax.plot_surface(x, y, 0*x+field_elevation, rstride=2, cstride=2, zorder=0, facecolors=img, linewidth=0.01)

print("Adding planes to plot...")
bigList=sorted_df.collect()
for i in bigList:
    ax.plot(i[2],i[1],i[3],linewidth=.03,zorder=10)

print("Saving plot as result.png...")
ax.set_xlim(-userRad/2,userRad/2)
ax.set_ylim(-userRad/2,userRad/2)
ax.set_zlim(field_elevation,userAlt)
plt.savefig('result.png',bbox_inches='tight', pad_inches=0,dpi=600)
print("done")

# In[ ]:

spark.stop()

# In[ ]: