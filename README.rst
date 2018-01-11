Add S3 support to dtool
=======================

S3 bucket policy
----------------


.. code-block:: json

    {
        "Id": "Policy1515674676844",
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Stmt1515667135964",
                "Action": [
                    "s3:ListBucket"
                ],
                "Effect": "Allow",
                "Resource": "arn:aws:s3:::test-dtool-s3-bucket",
                "Principal": {
                    "AWS": [
                        "arn:aws:iam::IAM_USER_HERE"
                    ]
                }
            },
            {
                "Sid": "Stmt1515674673516",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:DeleteObject"
                ],
                "Effect": "Allow",
                "Resource": "arn:aws:s3:::test-dtool-s3-bucket/*",
                "Principal": {
                    "AWS": [
                        "arn:aws:iam::IAM_USER_HERE"
                    ]
                }
            }
        ]
    }
