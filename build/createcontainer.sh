#!/bin/bash

# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

# setup specific to apsviz-maps
version=$1;

docker run -ti --name adcirc2cog_$version \
  --volume /d/dvols/apzviz:/data/sj37392jdj28538 \
  -d adcirc2cog:$version /bin/bash 
