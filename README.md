# Data-Analysis

<img src="https://images.unsplash.com/photo-1484069560501-87d72b0c3669?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=80" width="1100" height="500">

Image Source: [Unsplash](https://images.unsplash.com/photo-1484069560501-87d72b0c3669?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=800&q=80)

This repository contains projects regarding performing data analysis on the dataset. Projects included in the repository are:

1) **Flight Data Analysis**

This project involves scraping data from Bureau of Transportation Statistics and uploading the downloaded files onto the S3. This layer will act as the staging layer of the data. Next, ETL would be performed on this data on a EMR using Spark. The resultant data will be stored on the RedShift acting as a data warehouse. This data will be used as a source for making a dashboard in Tableau.

# LICENSE

MIT License

Copyright (c) 2020 Vinit Deshbhratar

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
