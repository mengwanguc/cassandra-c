/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "dc_aware_policy.hpp"

#include "logger.hpp"
#include "request_handler.hpp"
#include "scoped_lock.hpp"

#include <algorithm>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

int zipfian[10000] = {0, 4, 3, 5, 4, 8, 0, 0, 0, 4, 0, 0, 9, 3, 1, 0, 1, 5, 8, 0, 0, 6, 0, 0, 3, 0, 0, 6, 5, 0, 7, 2, 3, 0, 9, 9, 2, 0, 5, 4, 0, 2, 0, 5, 0, 4, 0, 3, 3, 0, 1, 1, 1, 0, 0, 9, 7, 1, 9, 2, 9, 6, 1, 4, 1, 3, 6, 6, 9, 1, 0, 0, 5, 0, 1, 0, 0, 3, 2, 3, 0, 3, 3, 0, 1, 8, 3, 0, 3, 2, 1, 8, 7, 0, 1, 6, 3, 0, 4, 0, 2, 1, 0, 6, 8, 0, 5, 8, 0, 1, 0, 2, 0, 9, 1, 2, 1, 4, 0, 6, 8, 6, 0, 0, 6, 7, 5, 5, 0, 0, 3, 3, 0, 0, 1, 6, 7, 6, 0, 0, 0, 2, 0, 2, 6, 0, 5, 2, 1, 4, 0, 1, 2, 5, 3, 5, 2, 0, 0, 4, 2, 4, 5, 0, 1, 8, 5, 3, 6, 6, 0, 4, 0, 0, 2, 4, 1, 1, 1, 0, 1, 7, 1, 2, 6, 4, 1, 5, 3, 0, 0, 0, 0, 0, 3, 2, 6, 8, 0, 2, 2, 2, 4, 0, 9, 1, 0, 0, 2, 0, 1, 7, 0, 0, 7, 1, 1, 2, 4, 4, 1, 3, 0, 6, 4, 2, 0, 0, 7, 2, 0, 7, 4, 0, 1, 1, 7, 1, 0, 0, 2, 5, 4, 1, 7, 0, 0, 1, 0, 0, 3, 7, 1, 4, 0, 0, 0, 3, 7, 7, 8, 0, 2, 1, 1, 0, 2, 0, 1, 3, 0, 0, 0, 0, 2, 0, 6, 2, 0, 9, 1, 3, 9, 2, 8, 4, 0, 2, 6, 2, 2, 2, 0, 1, 0, 3, 8, 3, 0, 0, 1, 7, 0, 9, 1, 3, 0, 2, 0, 7, 0, 2, 1, 5, 0, 8, 0, 7, 4, 0, 1, 0, 5, 1, 0, 0, 0, 1, 0, 7, 5, 0, 9, 0, 1, 0, 1, 0, 9, 0, 0, 3, 0, 0, 7, 6, 4, 4, 5, 2, 0, 1, 4, 0, 6, 1, 5, 0, 7, 9, 1, 2, 0, 6, 0, 7, 6, 7, 4, 9, 3, 0, 0, 6, 5, 0, 0, 0, 1, 2, 7, 7, 1, 8, 2, 4, 0, 1, 8, 0, 4, 1, 0, 6, 5, 0, 0, 9, 3, 0, 0, 0, 0, 4, 2, 6, 0, 2, 7, 6, 9, 0, 7, 2, 2, 1, 4, 4, 9, 0, 7, 4, 0, 3, 1, 9, 3, 7, 4, 9, 0, 7, 9, 2, 7, 0, 3, 4, 0, 4, 8, 3, 0, 9, 0, 0, 0, 1, 0, 1, 5, 0, 5, 2, 5, 0, 0, 2, 4, 0, 1, 2, 0, 3, 0, 8, 5, 3, 0, 5, 1, 8, 4, 2, 3, 2, 5, 3, 1, 8, 0, 0, 8, 0, 5, 0, 6, 4, 5, 6, 6, 5, 3, 1, 4, 1, 6, 6, 2, 0, 0, 2, 2, 2, 6, 1, 0, 1, 7, 0, 5, 0, 4, 4, 0, 4, 0, 4, 0, 1, 0, 0, 0, 2, 0, 9, 3, 3, 9, 2, 0, 1, 0, 5, 8, 5, 1, 9, 2, 6, 3, 0, 2, 6, 1, 3, 0, 3, 1, 0, 4, 7, 4, 0, 9, 1, 0, 0, 0, 0, 0, 0, 8, 1, 0, 4, 0, 0, 0, 0, 3, 2, 8, 0, 0, 3, 7, 6, 0, 0, 4, 9, 0, 0, 7, 3, 0, 2, 1, 5, 5, 1, 0, 1, 2, 5, 5, 0, 1, 4, 6, 3, 0, 6, 4, 3, 1, 5, 6, 0, 2, 0, 2, 4, 2, 5, 3, 5, 7, 0, 2, 0, 5, 0, 0, 4, 8, 2, 3, 7, 1, 0, 6, 0, 1, 8, 5, 0, 0, 2, 2, 4, 0, 2, 1, 5, 0, 0, 0, 9, 5, 1, 0, 1, 1, 0, 0, 0, 3, 3, 0, 0, 6, 0, 9, 4, 3, 0, 3, 7, 0, 5, 1, 0, 2, 2, 8, 0, 7, 9, 5, 1, 6, 0, 0, 0, 8, 3, 0, 5, 5, 4, 0, 9, 8, 6, 2, 1, 0, 4, 2, 1, 3, 6, 4, 5, 8, 9, 0, 6, 0, 5, 4, 4, 4, 7, 8, 0, 9, 0, 0, 3, 4, 3, 0, 3, 4, 1, 1, 3, 8, 6, 4, 9, 3, 4, 0, 0, 5, 1, 4, 1, 4, 0, 0, 2, 8, 2, 0, 0, 0, 0, 0, 6, 6, 2, 8, 0, 6, 4, 3, 1, 8, 6, 2, 3, 1, 1, 0, 0, 6, 6, 2, 4, 7, 2, 2, 0, 4, 5, 0, 3, 0, 1, 0, 3, 0, 1, 0, 0, 5, 4, 1, 1, 0, 8, 7, 5, 3, 4, 2, 1, 1, 0, 7, 6, 0, 1, 2, 1, 2, 0, 0, 1, 0, 9, 7, 5, 0, 2, 0, 6, 2, 8, 9, 0, 5, 7, 2, 1, 9, 7, 5, 1, 9, 4, 4, 7, 1, 1, 5, 6, 0, 9, 2, 2, 9, 0, 2, 1, 4, 5, 6, 0, 0, 0, 9, 0, 0, 2, 2, 3, 5, 8, 1, 0, 1, 2, 0, 2, 9, 1, 4, 3, 4, 6, 0, 3, 5, 0, 0, 9, 6, 0, 0, 7, 1, 3, 0, 6, 0, 5, 0, 1, 4, 0, 0, 9, 1, 0, 3, 3, 4, 0, 0, 2, 2, 4, 8, 0, 2, 8, 2, 2, 0, 9, 5, 5, 1, 0, 2, 0, 9, 1, 4, 3, 3, 5, 0, 2, 6, 3, 9, 6, 2, 3, 3, 6, 1, 5, 2, 0, 1, 2, 3, 4, 1, 1, 0, 0, 7, 0, 1, 2, 9, 1, 0, 3, 3, 2, 1, 2, 5, 2, 1, 0, 0, 8, 5, 2, 2, 0, 0, 3, 3, 5, 0, 8, 0, 0, 1, 8, 3, 8, 2, 1, 1, 8, 2, 9, 6, 4, 5, 3, 6, 4, 3, 4, 0, 1, 0, 0, 2, 5, 1, 0, 1, 0, 8, 0, 5, 2, 3, 5, 4, 6, 0, 7, 2, 5, 8, 2, 2, 4, 5, 4, 0, 7, 5, 0, 2, 1, 2, 2, 5, 6, 4, 0, 8, 9, 7, 7, 6, 4, 7, 1, 0, 0, 0, 9, 0, 8, 5, 0, 5, 1, 8, 7, 6, 6, 0, 5, 0, 0, 1, 0, 9, 0, 9, 2, 8, 1, 0, 2, 1, 0, 8, 0, 0, 4, 7, 6, 0, 2, 0, 9, 1, 9, 2, 9, 2, 1, 4, 0, 2, 2, 8, 4, 7, 0, 2, 0, 1, 0, 0, 0, 0, 7, 2, 5, 6, 3, 4, 5, 0, 1, 0, 0, 7, 1, 0, 3, 9, 6, 1, 4, 1, 4, 4, 0, 2, 0, 0, 3, 0, 1, 5, 0, 0, 0, 2, 7, 1, 2, 6, 1, 9, 3, 2, 7, 0, 2, 0, 0, 3, 0, 7, 1, 3, 0, 5, 9, 3, 0, 5, 4, 3, 4, 0, 1, 0, 0, 4, 6, 3, 0, 3, 0, 1, 0, 0, 1, 0, 2, 0, 0, 0, 7, 7, 6, 8, 1, 2, 9, 6, 7, 0, 1, 3, 0, 2, 8, 1, 0, 0, 7, 1, 2, 0, 0, 0, 3, 4, 0, 2, 0, 7, 1, 0, 0, 3, 2, 8, 0, 1, 0, 2, 0, 3, 0, 1, 8, 3, 4, 3, 0, 9, 9, 0, 6, 9, 2, 2, 0, 7, 5, 0, 5, 0, 7, 5, 3, 8, 5, 7, 3, 4, 6, 1, 7, 4, 0, 8, 0, 0, 3, 7, 1, 9, 0, 0, 0, 2, 1, 9, 3, 9, 0, 0, 0, 7, 0, 1, 3, 0, 0, 4, 1, 5, 2, 6, 6, 0, 0, 0, 5, 3, 7, 7, 2, 0, 0, 7, 0, 6, 0, 9, 0, 0, 0, 9, 0, 0, 2, 0, 1, 3, 5, 1, 7, 2, 1, 8, 3, 3, 1, 0, 0, 0, 0, 6, 0, 7, 2, 0, 8, 2, 6, 1, 0, 5, 6, 5, 0, 0, 9, 2, 2, 9, 7, 0, 7, 8, 1, 1, 0, 0, 9, 2, 9, 3, 4, 1, 0, 6, 9, 5, 2, 0, 4, 1, 0, 8, 6, 5, 0, 0, 6, 1, 0, 1, 1, 6, 5, 3, 0, 7, 0, 0, 2, 0, 2, 0, 3, 0, 0, 2, 1, 0, 4, 8, 0, 7, 0, 5, 5, 0, 6, 6, 1, 3, 0, 1, 7, 2, 3, 1, 1, 5, 5, 8, 0, 0, 5, 6, 4, 6, 0, 2, 2, 9, 1, 7, 3, 4, 0, 2, 9, 6, 2, 0, 0, 2, 0, 2, 0, 3, 4, 9, 5, 3, 6, 3, 0, 1, 0, 0, 1, 0, 3, 7, 0, 6, 6, 4, 2, 8, 0, 6, 5, 0, 3, 2, 3, 0, 9, 1, 0, 0, 0, 5, 7, 6, 4, 8, 0, 0, 0, 8, 0, 0, 3, 2, 0, 3, 0, 5, 1, 0, 0, 8, 1, 0, 1, 0, 0, 0, 6, 2, 0, 0, 7, 0, 0, 0, 8, 9, 1, 1, 0, 0, 0, 5, 9, 0, 0, 1, 2, 3, 6, 4, 0, 6, 0, 0, 1, 7, 6, 0, 4, 1, 3, 1, 1, 0, 2, 4, 2, 2, 0, 0, 1, 0, 2, 0, 0, 0, 0, 1, 9, 5, 1, 9, 2, 0, 0, 0, 4, 0, 0, 4, 4, 2, 6, 6, 8, 0, 5, 0, 0, 0, 8, 4, 0, 2, 3, 4, 3, 0, 3, 1, 0, 0, 0, 1, 4, 0, 4, 1, 8, 3, 0, 5, 3, 1, 2, 8, 0, 0, 4, 3, 8, 4, 0, 0, 1, 2, 3, 1, 1, 0, 0, 0, 5, 5, 0, 9, 0, 4, 6, 1, 3, 2, 5, 1, 3, 9, 3, 3, 4, 8, 0, 4, 0, 0, 6, 5, 0, 5, 8, 1, 0, 5, 5, 3, 7, 3, 0, 4, 3, 9, 7, 4, 0, 3, 9, 0, 2, 4, 2, 0, 4, 9, 0, 8, 2, 0, 1, 0, 1, 1, 1, 0, 0, 2, 0, 7, 0, 0, 0, 1, 6, 2, 9, 3, 9, 4, 0, 5, 0, 3, 0, 6, 1, 9, 9, 8, 9, 2, 1, 0, 0, 1, 1, 4, 0, 5, 3, 3, 0, 2, 1, 2, 7, 5, 7, 0, 0, 4, 0, 0, 1, 2, 0, 9, 1, 9, 5, 9, 6, 2, 8, 9, 0, 3, 0, 0, 1, 2, 0, 3, 4, 9, 4, 5, 0, 7, 6, 2, 9, 0, 6, 3, 0, 2, 8, 1, 6, 7, 0, 2, 1, 0, 0, 4, 8, 5, 2, 0, 2, 1, 8, 0, 1, 1, 1, 4, 6, 2, 1, 0, 9, 3, 0, 8, 7, 0, 1, 0, 0, 4, 5, 3, 2, 9, 0, 2, 0, 1, 0, 0, 2, 1, 4, 3, 7, 3, 0, 0, 4, 0, 0, 6, 8, 9, 0, 1, 8, 2, 0, 0, 1, 5, 1, 2, 4, 7, 8, 2, 0, 2, 8, 0, 0, 0, 4, 8, 2, 2, 0, 4, 0, 7, 3, 8, 0, 1, 0, 0, 2, 4, 0, 1, 5, 2, 2, 3, 0, 4, 7, 1, 0, 5, 0, 0, 3, 0, 0, 0, 1, 4, 0, 0, 6, 1, 0, 1, 8, 0, 1, 0, 4, 2, 3, 1, 0, 0, 1, 5, 0, 4, 0, 0, 0, 1, 6, 0, 5, 0, 6, 3, 0, 7, 2, 4, 7, 2, 2, 0, 5, 1, 1, 1, 1, 0, 4, 0, 1, 6, 7, 1, 0, 1, 1, 7, 0, 0, 9, 1, 4, 4, 0, 5, 3, 0, 8, 8, 6, 7, 0, 8, 0, 0, 1, 1, 5, 6, 6, 4, 3, 4, 4, 0, 4, 2, 0, 0, 9, 0, 6, 6, 2, 2, 0, 0, 1, 6, 3, 0, 2, 2, 0, 8, 0, 0, 0, 0, 6, 3, 2, 0, 0, 9, 6, 0, 0, 5, 9, 6, 1, 2, 9, 0, 0, 2, 6, 7, 1, 0, 0, 2, 0, 0, 9, 0, 2, 6, 3, 0, 1, 0, 4, 5, 0, 4, 1, 9, 4, 2, 2, 1, 2, 3, 1, 0, 1, 9, 0, 3, 0, 7, 2, 7, 0, 8, 4, 5, 3, 1, 9, 0, 7, 0, 6, 0, 0, 4, 0, 8, 6, 0, 0, 1, 7, 5, 3, 9, 5, 4, 8, 1, 0, 0, 1, 8, 0, 4, 6, 1, 9, 6, 6, 0, 4, 0, 4, 0, 0, 1, 3, 4, 0, 4, 1, 0, 6, 8, 0, 1, 6, 0, 8, 8, 1, 9, 7, 0, 2, 3, 0, 6, 9, 7, 1, 2, 0, 7, 9, 2, 3, 9, 0, 1, 6, 2, 0, 0, 5, 6, 2, 7, 9, 1, 5, 5, 3, 5, 7, 0, 0, 8, 7, 2, 0, 0, 1, 0, 3, 2, 0, 1, 0, 5, 3, 4, 7, 4, 0, 4, 2, 1, 7, 3, 1, 5, 0, 2, 1, 1, 2, 0, 3, 0, 7, 0, 5, 5, 0, 6, 6, 4, 6, 4, 8, 0, 9, 6, 0, 4, 7, 5, 0, 0, 0, 0, 2, 9, 2, 0, 4, 1, 0, 5, 9, 2, 8, 5, 0, 4, 6, 0, 7, 0, 4, 0, 2, 0, 5, 5, 0, 4, 0, 4, 0, 0, 0, 2, 6, 0, 7, 0, 0, 2, 0, 5, 0, 1, 0, 0, 1, 2, 1, 0, 0, 8, 3, 0, 2, 1, 8, 7, 3, 0, 2, 2, 3, 2, 9, 2, 7, 1, 4, 0, 4, 3, 6, 7, 0, 6, 8, 5, 0, 6, 3, 4, 9, 0, 0, 1, 2, 0, 0, 4, 0, 4, 7, 0, 4, 0, 8, 4, 0, 8, 7, 0, 4, 0, 0, 1, 3, 1, 8, 0, 1, 0, 2, 3, 1, 9, 5, 3, 8, 0, 1, 0, 0, 7, 4, 5, 2, 5, 0, 1, 7, 7, 0, 9, 2, 0, 1, 5, 6, 3, 1, 8, 9, 0, 9, 6, 0, 1, 1, 4, 0, 3, 0, 8, 0, 7, 1, 3, 8, 0, 2, 0, 0, 6, 7, 3, 0, 0, 1, 1, 3, 2, 0, 0, 6, 7, 0, 5, 9, 9, 0, 0, 0, 0, 6, 6, 0, 4, 1, 0, 1, 5, 0, 0, 0, 0, 2, 0, 0, 7, 2, 0, 1, 9, 3, 2, 0, 0, 0, 1, 1, 0, 0, 2, 6, 1, 0, 5, 2, 0, 4, 8, 7, 1, 2, 7, 9, 0, 1, 7, 1, 2, 1, 1, 5, 0, 1, 2, 0, 1, 2, 1, 1, 0, 0, 7, 0, 2, 3, 1, 2, 7, 5, 0, 3, 2, 7, 6, 3, 2, 1, 0, 0, 4, 2, 3, 9, 7, 0, 5, 5, 9, 1, 0, 4, 5, 0, 0, 6, 1, 2, 2, 1, 2, 3, 0, 0, 5, 8, 0, 8, 2, 4, 4, 5, 0, 0, 0, 3, 2, 0, 4, 2, 0, 5, 0, 0, 9, 1, 1, 8, 0, 0, 7, 8, 2, 8, 8, 5, 0, 8, 0, 6, 1, 5, 3, 5, 0, 3, 5, 2, 4, 6, 2, 4, 0, 0, 2, 4, 6, 5, 7, 0, 4, 0, 0, 2, 0, 0, 3, 0, 8, 6, 1, 0, 4, 8, 1, 1, 0, 5, 2, 1, 3, 7, 7, 2, 0, 2, 2, 0, 9, 6, 0, 4, 1, 0, 6, 7, 9, 1, 0, 2, 9, 5, 5, 2, 5, 0, 8, 8, 0, 0, 2, 0, 8, 2, 9, 0, 1, 9, 4, 1, 0, 9, 2, 6, 0, 0, 7, 7, 9, 5, 1, 0, 7, 0, 4, 3, 8, 2, 5, 7, 0, 8, 1, 2, 0, 0, 4, 1, 0, 4, 2, 0, 7, 0, 8, 6, 8, 0, 0, 9, 7, 0, 0, 2, 2, 1, 2, 2, 1, 0, 0, 1, 1, 2, 1, 0, 6, 6, 7, 0, 0, 1, 3, 4, 2, 2, 3, 0, 7, 1, 0, 2, 1, 0, 0, 3, 0, 0, 1, 0, 5, 3, 2, 1, 4, 3, 4, 3, 8, 1, 1, 9, 0, 4, 1, 1, 0, 1, 0, 5, 3, 9, 1, 2, 0, 2, 2, 8, 0, 0, 7, 0, 7, 4, 4, 7, 1, 9, 0, 1, 6, 0, 3, 4, 0, 9, 4, 0, 7, 0, 0, 1, 4, 2, 0, 3, 9, 3, 2, 1, 2, 1, 2, 5, 0, 0, 1, 9, 5, 9, 0, 1, 0, 0, 1, 6, 1, 5, 0, 1, 1, 4, 0, 5, 0, 4, 1, 2, 4, 0, 0, 2, 7, 6, 0, 3, 0, 1, 1, 5, 5, 0, 0, 0, 2, 3, 8, 0, 4, 4, 0, 0, 0, 2, 0, 9, 0, 0, 0, 4, 0, 4, 0, 6, 0, 1, 0, 1, 0, 1, 0, 6, 0, 2, 8, 0, 0, 4, 2, 2, 1, 4, 0, 2, 3, 1, 4, 2, 7, 6, 2, 0, 6, 7, 3, 0, 0, 2, 1, 0, 3, 1, 0, 4, 4, 9, 6, 4, 3, 2, 1, 5, 0, 3, 7, 2, 3, 2, 0, 4, 2, 0, 3, 1, 3, 3, 0, 1, 8, 0, 0, 6, 2, 6, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, 8, 0, 2, 9, 0, 6, 5, 4, 0, 2, 8, 0, 0, 0, 1, 7, 0, 1, 2, 4, 0, 0, 1, 5, 8, 0, 2, 0, 1, 7, 5, 0, 0, 1, 1, 8, 0, 0, 0, 2, 0, 4, 0, 0, 6, 2, 9, 6, 0, 0, 0, 2, 5, 8, 6, 0, 0, 2, 6, 2, 8, 2, 0, 3, 6, 5, 6, 7, 5, 0, 5, 8, 0, 5, 1, 0, 3, 5, 0, 0, 0, 5, 5, 3, 9, 5, 1, 0, 3, 1, 5, 0, 0, 7, 5, 5, 4, 3, 1, 1, 8, 0, 6, 3, 7, 1, 0, 0, 0, 7, 4, 5, 0, 8, 4, 7, 5, 0, 8, 0, 4, 0, 0, 0, 0, 4, 9, 4, 6, 0, 2, 0, 9, 0, 2, 0, 3, 3, 2, 1, 0, 4, 5, 0, 2, 7, 0, 0, 0, 9, 7, 0, 3, 1, 3, 0, 2, 7, 0, 2, 2, 0, 8, 0, 9, 7, 0, 0, 0, 2, 0, 7, 7, 0, 4, 0, 4, 6, 7, 0, 0, 2, 0, 0, 1, 5, 6, 7, 8, 0, 3, 1, 0, 8, 0, 0, 2, 9, 7, 0, 4, 5, 5, 0, 0, 7, 1, 7, 2, 2, 4, 6, 0, 2, 5, 6, 1, 1, 7, 3, 7, 0, 0, 0, 3, 0, 0, 2, 5, 4, 1, 1, 8, 9, 4, 8, 0, 9, 0, 5, 8, 3, 8, 2, 4, 2, 0, 3, 7, 0, 5, 0, 1, 0, 0, 6, 0, 0, 4, 5, 2, 1, 7, 1, 5, 1, 5, 0, 0, 6, 0, 0, 2, 0, 1, 9, 1, 0, 3, 7, 0, 2, 0, 5, 8, 2, 0, 6, 1, 5, 0, 7, 6, 5, 1, 3, 0, 7, 1, 6, 0, 0, 8, 0, 3, 4, 4, 7, 3, 1, 8, 2, 0, 0, 0, 0, 0, 1, 0, 3, 0, 1, 8, 9, 1, 8, 0, 9, 7, 7, 0, 3, 0, 0, 0, 2, 7, 4, 1, 1, 4, 9, 8, 1, 4, 0, 0, 0, 7, 5, 2, 2, 8, 2, 5, 7, 8, 3, 0, 4, 3, 2, 3, 4, 2, 0, 0, 0, 1, 0, 2, 1, 9, 8, 0, 7, 1, 7, 0, 9, 0, 0, 0, 1, 6, 1, 1, 1, 4, 0, 1, 0, 1, 1, 7, 1, 3, 1, 0, 9, 2, 0, 6, 4, 8, 0, 1, 5, 1, 4, 5, 7, 4, 0, 4, 0, 0, 0, 4, 2, 0, 7, 7, 2, 8, 8, 9, 8, 0, 4, 5, 1, 0, 2, 0, 0, 5, 7, 0, 0, 0, 0, 0, 4, 1, 0, 4, 6, 0, 5, 8, 5, 5, 0, 9, 0, 6, 0, 2, 0, 3, 7, 5, 1, 9, 2, 1, 4, 1, 1, 1, 2, 8, 7, 2, 0, 7, 3, 6, 0, 9, 1, 0, 8, 0, 1, 0, 5, 4, 6, 0, 6, 0, 1, 3, 0, 0, 4, 3, 3, 0, 0, 0, 1, 1, 1, 3, 3, 5, 2, 5, 0, 1, 0, 0, 9, 0, 1, 7, 2, 4, 1, 1, 4, 0, 0, 4, 6, 0, 0, 4, 1, 0, 1, 7, 2, 0, 4, 3, 7, 0, 1, 1, 5, 6, 3, 0, 4, 0, 7, 1, 2, 2, 1, 0, 2, 4, 2, 9, 8, 8, 4, 2, 5, 2, 2, 7, 1, 6, 4, 0, 2, 0, 4, 3, 8, 5, 5, 0, 3, 2, 1, 0, 1, 1, 8, 4, 1, 0, 1, 0, 7, 7, 3, 0, 8, 0, 1, 1, 5, 0, 3, 1, 7, 0, 7, 4, 4, 0, 1, 0, 2, 1, 0, 0, 2, 2, 1, 0, 6, 6, 0, 9, 4, 4, 0, 1, 9, 0, 3, 2, 1, 2, 0, 8, 5, 1, 6, 0, 0, 0, 6, 1, 1, 6, 6, 0, 0, 0, 3, 3, 5, 8, 5, 0, 6, 0, 0, 2, 2, 0, 3, 1, 0, 4, 2, 2, 0, 0, 2, 7, 2, 0, 0, 7, 0, 2, 0, 6, 0, 7, 1, 2, 3, 0, 1, 6, 1, 0, 3, 2, 0, 1, 7, 3, 5, 0, 1, 3, 8, 0, 0, 9, 8, 6, 3, 2, 1, 5, 4, 2, 0, 0, 5, 5, 0, 3, 3, 3, 2, 0, 0, 0, 0, 3, 4, 2, 3, 1, 0, 0, 1, 6, 1, 2, 6, 6, 7, 3, 2, 8, 7, 1, 3, 0, 1, 4, 1, 2, 7, 2, 0, 9, 5, 0, 0, 2, 4, 8, 6, 5, 3, 0, 0, 0, 0, 9, 3, 1, 0, 4, 7, 1, 3, 5, 0, 3, 6, 6, 0, 0, 1, 9, 9, 3, 0, 0, 9, 0, 6, 3, 7, 1, 0, 2, 7, 0, 5, 8, 6, 1, 3, 9, 0, 3, 2, 0, 1, 6, 0, 9, 5, 1, 0, 0, 3, 0, 1, 0, 0, 8, 9, 2, 0, 1, 1, 1, 3, 1, 8, 6, 7, 1, 0, 2, 2, 0, 8, 2, 3, 5, 4, 6, 0, 7, 0, 3, 2, 5, 0, 9, 0, 3, 6, 5, 0, 9, 9, 1, 6, 0, 3, 9, 4, 1, 7, 8, 2, 0, 4, 0, 1, 2, 0, 5, 1, 3, 5, 3, 3, 2, 1, 5, 1, 0, 0, 3, 5, 1, 2, 2, 1, 3, 0, 0, 1, 0, 0, 5, 9, 0, 5, 9, 7, 3, 6, 8, 1, 0, 1, 1, 1, 2, 1, 0, 0, 9, 1, 1, 1, 0, 9, 0, 5, 2, 0, 0, 5, 6, 0, 3, 0, 0, 6, 0, 0, 0, 7, 8, 3, 6, 9, 0, 0, 9, 5, 8, 1, 0, 1, 0, 6, 2, 3, 9, 0, 0, 6, 9, 0, 7, 3, 0, 3, 2, 6, 0, 0, 2, 5, 8, 2, 1, 0, 6, 0, 0, 6, 1, 1, 0, 0, 0, 3, 0, 1, 9, 0, 5, 9, 0, 4, 0, 0, 2, 3, 5, 3, 0, 0, 0, 0, 7, 8, 1, 0, 2, 0, 3, 8, 3, 1, 9, 3, 2, 9, 3, 1, 5, 0, 2, 0, 9, 9, 3, 7, 4, 1, 0, 3, 1, 3, 4, 0, 0, 0, 3, 1, 8, 1, 4, 1, 4, 4, 8, 6, 0, 1, 0, 1, 2, 6, 6, 1, 2, 0, 4, 7, 4, 4, 4, 7, 1, 0, 0, 0, 5, 5, 2, 3, 3, 4, 1, 3, 0, 2, 9, 1, 9, 2, 3, 1, 1, 9, 9, 0, 2, 0, 0, 7, 1, 3, 4, 0, 0, 1, 1, 7, 0, 8, 0, 4, 1, 5, 8, 8, 0, 0, 0, 2, 4, 8, 6, 0, 5, 0, 0, 7, 0, 4, 9, 1, 3, 8, 0, 2, 0, 9, 1, 0, 8, 3, 6, 2, 1, 8, 2, 5, 2, 4, 1, 5, 1, 0, 2, 5, 2, 3, 1, 1, 5, 8, 4, 2, 5, 0, 0, 0, 6, 5, 1, 0, 7, 9, 0, 4, 3, 2, 8, 2, 1, 9, 1, 0, 0, 2, 0, 1, 6, 9, 4, 0, 1, 7, 0, 2, 4, 7, 2, 1, 9, 0, 3, 0, 2, 3, 3, 2, 7, 0, 3, 0, 0, 4, 0, 0, 0, 0, 7, 4, 0, 1, 0, 0, 8, 6, 4, 9, 0, 0, 6, 5, 2, 6, 1, 8, 3, 1, 0, 6, 2, 9, 2, 0, 2, 1, 3, 6, 2, 0, 2, 4, 0, 9, 2, 0, 9, 0, 5, 7, 0, 9, 0, 7, 0, 2, 0, 2, 4, 0, 2, 5, 0, 3, 0, 0, 3, 3, 9, 1, 3, 3, 1, 0, 6, 2, 3, 4, 2, 5, 6, 6, 0, 2, 0, 0, 2, 2, 0, 3, 1, 2, 5, 1, 5, 0, 8, 0, 5, 0, 5, 0, 6, 0, 2, 1, 1, 4, 0, 3, 0, 0, 0, 2, 0, 0, 5, 0, 1, 0, 0, 3, 1, 0, 8, 2, 0, 6, 0, 0, 0, 0, 2, 6, 1, 2, 0, 0, 1, 6, 2, 5, 4, 7, 4, 6, 6, 0, 6, 2, 0, 5, 0, 0, 0, 4, 0, 0, 7, 1, 2, 1, 3, 1, 7, 1, 0, 3, 0, 8, 9, 9, 0, 2, 1, 0, 0, 1, 1, 1, 0, 0, 6, 1, 0, 0, 1, 4, 0, 6, 9, 7, 1, 0, 6, 6, 1, 1, 1, 2, 9, 7, 5, 0, 0, 6, 0, 1, 0, 2, 2, 4, 5, 5, 0, 5, 1, 8, 0, 4, 0, 1, 1, 0, 1, 1, 8, 3, 2, 1, 1, 8, 0, 1, 1, 7, 4, 9, 6, 0, 0, 6, 5, 3, 0, 5, 2, 8, 6, 8, 0, 2, 3, 2, 1, 0, 5, 0, 7, 1, 1, 1, 0, 2, 0, 9, 8, 2, 6, 5, 8, 7, 0, 2, 2, 3, 2, 1, 0, 0, 8, 0, 1, 8, 4, 5, 3, 1, 2, 1, 4, 0, 3, 0, 2, 0, 2, 9, 3, 3, 2, 2, 4, 0, 0, 0, 4, 5, 4, 0, 8, 0, 0, 8, 9, 1, 6, 5, 1, 6, 6, 0, 0, 9, 0, 0, 1, 7, 0, 0, 5, 7, 5, 4, 0, 0, 7, 0, 3, 0, 0, 2, 4, 0, 2, 0, 6, 7, 7, 0, 6, 1, 0, 0, 8, 9, 1, 2, 2, 2, 0, 9, 1, 6, 9, 7, 0, 0, 2, 5, 0, 2, 8, 0, 0, 0, 5, 2, 6, 5, 0, 3, 1, 5, 8, 3, 0, 0, 1, 4, 2, 2, 1, 0, 0, 0, 9, 0, 1, 6, 6, 6, 0, 2, 8, 0, 0, 1, 5, 5, 6, 1, 0, 0, 5, 5, 0, 7, 7, 9, 0, 0, 0, 3, 6, 7, 7, 2, 0, 4, 0, 0, 9, 2, 2, 5, 9, 1, 3, 2, 9, 2, 7, 2, 5, 2, 7, 0, 4, 3, 3, 2, 0, 5, 0, 9, 2, 0, 9, 1, 0, 8, 3, 7, 0, 0, 4, 1, 0, 0, 1, 6, 9, 5, 6, 5, 1, 2, 0, 4, 6, 0, 8, 1, 0, 0, 0, 3, 5, 5, 6, 4, 1, 1, 2, 2, 5, 0, 3, 1, 5, 3, 0, 1, 0, 1, 0, 8, 0, 0, 1, 9, 3, 6, 7, 8, 0, 0, 0, 8, 6, 1, 2, 8, 3, 3, 1, 0, 0, 0, 9, 6, 4, 6, 3, 0, 1, 9, 7, 7, 7, 3, 0, 7, 0, 6, 2, 0, 2, 0, 0, 2, 9, 6, 0, 2, 1, 7, 2, 0, 5, 2, 0, 1, 0, 1, 8, 0, 0, 0, 6, 0, 0, 0, 7, 7, 0, 1, 0, 3, 0, 0, 9, 0, 3, 1, 1, 0, 1, 0, 1, 5, 0, 0, 2, 0, 4, 0, 1, 1, 8, 0, 0, 0, 6, 0, 5, 4, 6, 3, 0, 8, 5, 0, 8, 1, 0, 0, 4, 2, 2, 0, 6, 0, 0, 0, 6, 3, 0, 0, 9, 3, 0, 0, 0, 8, 8, 9, 2, 0, 3, 0, 8, 1, 6, 1, 3, 0, 1, 3, 2, 6, 7, 3, 6, 9, 0, 9, 0, 4, 6, 0, 0, 0, 0, 3, 0, 5, 4, 6, 0, 1, 9, 0, 2, 0, 1, 0, 7, 1, 4, 1, 8, 0, 0, 6, 4, 9, 1, 5, 0, 2, 0, 6, 0, 0, 0, 4, 4, 3, 5, 3, 2, 1, 0, 0, 4, 5, 3, 2, 0, 4, 1, 8, 0, 2, 1, 1, 6, 7, 7, 6, 7, 0, 9, 3, 2, 5, 0, 9, 0, 9, 8, 0, 0, 0, 8, 4, 0, 6, 0, 0, 0, 0, 1, 0, 1, 4, 5, 5, 0, 1, 0, 3, 0, 2, 0, 1, 3, 0, 1, 1, 6, 1, 6, 6, 5, 0, 0, 0, 9, 5, 0, 1, 6, 5, 8, 0, 8, 0, 1, 1, 8, 1, 0, 8, 0, 0, 8, 0, 2, 0, 1, 6, 2, 0, 9, 4, 0, 0, 0, 0, 5, 3, 2, 0, 6, 0, 2, 5, 4, 4, 0, 5, 2, 2, 7, 0, 2, 4, 7, 2, 0, 8, 0, 0, 0, 6, 6, 2, 3, 1, 0, 1, 4, 8, 2, 0, 0, 7, 0, 6, 1, 8, 5, 6, 1, 1, 0, 0, 7, 0, 0, 7, 2, 9, 0, 0, 0, 1, 0, 0, 5, 0, 0, 1, 4, 0, 0, 0, 6, 6, 6, 0, 3, 0, 6, 0, 0, 0, 6, 7, 7, 3, 4, 0, 1, 0, 3, 5, 7, 7, 4, 0, 6, 4, 0, 3, 0, 0, 2, 0, 2, 4, 1, 8, 2, 1, 1, 5, 0, 7, 0, 1, 2, 9, 9, 6, 0, 0, 5, 0, 1, 1, 1, 2, 3, 0, 0, 3, 5, 0, 4, 0, 0, 1, 9, 1, 0, 5, 8, 0, 1, 0, 5, 2, 0, 0, 0, 0, 2, 0, 0, 0, 8, 2, 2, 0, 9, 7, 0, 1, 4, 0, 3, 7, 2, 0, 1, 2, 0, 8, 5, 7, 1, 4, 1, 2, 2, 0, 3, 5, 0, 0, 2, 6, 2, 5, 1, 3, 4, 7, 9, 0, 5, 2, 0, 5, 5, 1, 7, 1, 4, 5, 2, 7, 9, 1, 4, 7, 1, 4, 0, 0, 1, 0, 8, 1, 6, 1, 3, 0, 1, 5, 0, 1, 1, 0, 7, 6, 4, 0, 4, 4, 1, 2, 0, 3, 0, 0, 6, 5, 4, 4, 0, 0, 4, 0, 8, 7, 3, 3, 0, 4, 8, 0, 3, 9, 5, 1, 6, 7, 1, 1, 0, 1, 5, 0, 2, 8, 2, 0, 0, 2, 4, 4, 0, 3, 4, 2, 0, 0, 8, 4, 3, 0, 7, 0, 0, 3, 4, 0, 7, 2, 1, 1, 7, 0, 9, 4, 1, 1, 2, 0, 4, 1, 0, 5, 5, 0, 0, 2, 4, 1, 0, 0, 4, 4, 8, 2, 3, 0, 0, 3, 9, 4, 0, 6, 3, 2, 3, 2, 4, 0, 9, 2, 2, 0, 2, 0, 0, 3, 7, 0, 5, 8, 8, 0, 4, 7, 7, 2, 4, 1, 0, 0, 7, 5, 0, 0, 0, 8, 1, 4, 0, 7, 0, 9, 0, 0, 0, 2, 0, 0, 8, 4, 3, 1, 0, 2, 0, 4, 0, 0, 1, 2, 0, 0, 2, 7, 5, 6, 6, 0, 0, 0, 4, 0, 9, 4, 4, 0, 9, 0, 4, 0, 2, 0, 1, 4, 1, 6, 0, 5, 3, 8, 3, 2, 0, 6, 7, 4, 0, 0, 0, 4, 0, 1, 6, 0, 7, 0, 1, 4, 0, 0, 2, 9, 3, 2, 0, 3, 0, 5, 0, 7, 0, 2, 3, 1, 9, 1, 1, 2, 6, 1, 2, 7, 0, 4, 0, 1, 0, 1, 3, 0, 3, 7, 1, 2, 9, 2, 2, 3, 5, 5, 9, 3, 1, 2, 6, 0, 0, 4, 2, 6, 0, 7, 6, 1, 1, 3, 5, 0, 0, 9, 0, 0, 3, 2, 7, 7, 1, 0, 5, 0, 4, 0, 6, 4, 7, 6, 0, 1, 1, 0, 0, 0, 0, 5, 0, 6, 0, 6, 1, 0, 5, 6, 1, 9, 1, 8, 0, 1, 2, 0, 4, 7, 6, 0, 0, 1, 0, 5, 4, 1, 2, 6, 1, 4, 2, 8, 3, 3, 7, 8, 9, 9, 6, 5, 8, 4, 3, 4, 0, 0, 3, 1, 3, 6, 3, 9, 0, 4, 0, 0, 5, 1, 3, 6, 6, 0, 0, 3, 7, 0, 0, 0, 4, 7, 2, 0, 0, 2, 4, 0, 4, 0, 0, 6, 1, 1, 0, 2, 0, 0, 2, 0, 6, 0, 8, 8, 3, 0, 3, 0, 1, 2, 3, 3, 7, 0, 4, 0, 0, 6, 2, 1, 4, 6, 6, 5, 5, 0, 0, 0, 6, 9, 0, 6, 9, 9, 3, 1, 0, 5, 0, 5, 1, 4, 4, 0, 1, 0, 8, 0, 1, 0, 5, 1, 8, 5, 0, 1, 0, 6, 8, 3, 0, 5, 7, 8, 8, 0, 4, 1, 4, 0, 8, 8, 0, 7, 0, 0, 0, 7, 7, 7, 0, 1, 2, 1, 4, 0, 5, 1, 2, 8, 4, 0, 9, 0, 1, 6, 0, 5, 0, 6, 1, 2, 2, 0, 0, 0, 1, 3, 0, 1, 3, 0, 1, 8, 3, 0, 4, 8, 4, 0, 0, 0, 0, 5, 7, 4, 0, 1, 1, 0, 1, 0, 6, 7, 6, 0, 2, 0, 1, 0, 5, 0, 0, 2, 9, 6, 2, 7, 0, 4, 7, 1, 5, 8, 3, 5, 1, 2, 4, 0, 0, 6, 0, 2, 2, 1, 0, 0, 4, 8, 3, 4, 2, 2, 0, 1, 3, 1, 0, 8, 0, 0, 5, 4, 2, 0, 2, 6, 2, 7, 1, 7, 2, 0, 3, 0, 8, 6, 8, 6, 2, 1, 0, 0, 1, 0, 0, 1, 0, 0, 3, 6, 2, 7, 7, 8, 8, 3, 3, 6, 0, 2, 0, 2, 6, 2, 0, 0, 0, 0, 0, 0, 1, 0, 3, 0, 0, 2, 3, 5, 4, 8, 0, 4, 8, 8, 3, 0, 1, 9, 0, 3, 0, 1, 4, 0, 0, 6, 2, 0, 6, 2, 2, 0, 3, 7, 0, 5, 0, 4, 2, 0, 3, 0, 0, 0, 6, 4, 8, 1, 3, 9, 8, 1, 1, 1, 3, 1, 1, 2, 0, 3, 5, 3, 0, 7, 2, 2, 6, 9, 9, 6, 6, 5, 1, 0, 6, 0, 3, 8, 0, 5, 0, 6, 0, 4, 5, 7, 0, 1, 4, 1, 0, 3, 7, 3, 4, 2, 0, 0, 0, 2, 4, 8, 5, 1, 0, 0, 5, 2, 3, 6, 3, 9, 3, 4, 0, 1, 4, 0, 6, 8, 5, 0, 1, 5, 0, 1, 1, 4, 7, 0, 2, 2, 4, 7, 0, 3, 0, 5, 1, 0, 0, 2, 0, 5, 6, 9, 2, 0, 5, 2, 3, 9, 9, 0, 0, 6, 1, 4, 6, 1, 3, 3, 3, 1, 7, 4, 0, 2, 8, 0, 5, 5, 4, 4, 5, 9, 9, 1, 8, 1, 3, 3, 2, 9, 0, 4, 9, 1, 0, 0, 1, 2, 0, 0, 0, 9, 4, 2, 6, 6, 1, 0, 7, 1, 4, 0, 1, 5, 7, 2, 7, 4, 0, 0, 4, 2, 7, 0, 6, 6, 5, 1, 1, 7, 3, 2, 3, 9, 5, 0, 1, 0, 0, 1, 8, 1, 0, 1, 4, 1, 9, 0, 9, 0, 0, 0, 5, 3, 0, 9, 2, 8, 5, 8, 3, 7, 3, 1, 6, 3, 9, 0, 5, 1, 0, 1, 2, 8, 6, 3, 0, 4, 0, 9, 2, 1, 5, 6, 1, 0, 7, 6, 7, 1, 0, 4, 1, 0, 9, 4, 1, 1, 9, 0, 8, 8, 5, 9, 9, 2, 9, 0, 1, 2, 1, 8, 3, 1, 1, 3, 6, 0, 0, 5, 0, 9, 5, 2, 0, 0, 6, 0, 4, 1, 9, 6, 7, 2, 0, 6, 7, 0, 2, 0, 3, 3, 5, 0, 2, 1, 6, 8, 4, 1, 2, 8, 3, 6, 5, 6, 1, 8, 0, 4, 3, 0, 0, 0, 0, 0, 2, 4, 0, 0, 5, 0, 0, 5, 5, 4, 6, 1, 0, 2, 7, 0, 0, 8, 0, 0, 0, 0, 2, 5, 0, 0, 0, 0, 0, 0, 8, 4, 0, 2, 0, 5, 0, 6, 0, 5, 1, 0, 5, 5, 1, 8, 1, 2, 1, 8, 2, 2, 7, 6, 0, 5, 1, 5, 9, 9, 5, 2, 4, 0, 6, 6, 4, 0, 9, 4, 0, 3, 4, 1, 8, 0, 0, 4, 1, 9, 1, 9, 0, 0, 3, 0, 0, 4, 2, 1, 8, 0, 7, 6, 3, 8, 0, 0, 0, 2, 0, 0, 8, 7, 3, 0, 9, 8, 0, 8, 0, 8, 0, 3, 0, 1, 0, 6, 8, 1, 8, 3, 0, 0, 7, 0, 0, 0, 5, 8, 1, 1, 7, 4, 0, 3, 3, 0, 0, 5, 0, 1, 7, 5, 2, 0, 1, 2, 5, 0, 1, 1, 4, 0, 2, 7, 2, 4, 2, 7, 0, 8, 3, 2, 7, 1, 7, 2, 0, 1, 1, 6, 0, 0, 6, 0, 3, 2, 0, 6, 0, 6, 4, 2, 5, 3, 8, 4, 0, 0, 0, 0, 4, 8, 0, 0, 0, 6, 0, 3, 0, 4, 0, 0, 0, 0, 2, 0, 8, 5, 3, 2, 2, 0, 9, 9, 4, 7, 6, 3, 5, 0, 0, 0, 1, 1, 0, 7, 0, 9, 9, 0, 4, 1, 9, 3, 8, 2, 1, 2, 0, 8, 0, 3, 1, 2, 2, 2, 0, 8, 3, 5, 8, 8, 2, 5, 2, 0, 2, 0, 7, 7, 0, 5, 0, 0, 0, 6, 2, 5, 0, 0, 2, 3, 3, 1, 1, 2, 5, 9, 2, 1, 2, 3, 4, 0, 1, 0, 0, 5, 5, 7, 7, 0, 7, 7, 4, 9, 0, 1, 2, 0, 7, 9, 0, 0, 9, 0, 8, 2, 5, 9, 2, 7, 2, 1, 9, 2, 4, 4, 2, 0, 0, 1, 0, 1, 3, 0, 4, 3, 1, 1, 2, 4, 0, 8, 5, 5, 7, 0, 9, 1, 7, 3, 5, 3, 0, 1, 2, 1, 1, 0, 6, 8, 1, 1, 7, 0, 6, 0, 4, 5, 9, 0, 1, 8, 2, 2, 0, 2, 6, 3, 9, 1, 7, 1, 3, 0, 0, 0, 0, 7, 0, 1, 3, 0, 4, 0, 0, 5, 4, 5, 7, 1, 4, 4, 1, 0, 0, 1, 3, 2, 4, 0, 0, 7, 1, 4, 8, 6, 1, 7, 9, 5, 0, 2, 5, 3, 7, 0, 1, 1, 8, 0, 5, 1, 2, 0, 5, 0, 0, 0, 6, 2, 7, 1, 1, 7, 5, 2, 0, 6, 7, 0, 3, 5, 3, 0, 0, 6, 0, 0, 0, 8, 2, 2, 0, 0, 1, 8, 1, 3, 0, 7, 0, 1, 0, 3, 1, 2, 1, 7, 1, 0, 1, 0, 7, 5, 1, 4, 3, 0, 0, 6, 7, 1, 1, 2, 0, 0, 0, 1, 3, 9, 5, 2, 1, 0, 0, 0, 1, 6, 1, 9, 8, 0, 0, 3, 0, 5, 8, 0, 0, 3, 2, 5, 0, 3, 3, 9, 0, 0, 2, 5, 0, 8, 0, 1, 7, 2, 0, 3, 2, 1, 5, 4, 4, 6, 6, 0, 0, 0, 2, 1, 0, 8, 2, 0, 8, 0, 4, 8, 3, 5, 0, 0, 8, 0, 0, 1, 5, 0, 2, 4, 1, 6, 3, 3, 2, 9, 0, 0, 3, 1, 4, 5, 3, 5, 9, 6, 4, 0, 7, 8, 0, 3, 6, 5, 1, 0, 4, 0, 5, 1, 1, 5, 0, 0, 5, 0, 4, 0, 7, 2, 0, 2, 0, 4, 1, 4, 0, 0, 7, 8, 0, 0, 5, 4, 0, 9, 1, 0, 2, 6, 0, 0, 5, 9, 0, 7, 0, 4, 5, 6, 5, 6, 2, 8, 0, 2, 4, 2, 0, 0, 0, 0, 1, 6, 7, 3, 0, 2, 5, 3, 5, 7, 2, 2, 0, 9, 1, 0, 1, 0, 1, 8, 0, 5, 0, 0, 1, 0, 7, 0, 1, 3, 1, 1, 0, 2, 1, 0, 4, 6, 1, 6, 0, 0, 0, 1, 0, 0, 6, 4, 2, 0, 3, 3, 2, 1, 4, 2, 0, 0, 1, 3, 7, 6, 1, 7, 5, 6, 0, 6, 2, 6, 0, 1, 9, 0, 5, 2, 0, 0, 3, 2, 4, 3, 0, 4, 0, 2, 5, 0, 7, 8, 7, 6, 5, 3, 1, 1, 3, 3, 5, 1, 1, 0, 7, 4, 0, 6, 8, 0, 2, 1, 5, 7, 0, 2, 4, 4, 2, 2, 5, 8, 3, 1, 0, 2, 1, 0, 0, 0, 6, 2, 0, 1, 1, 0, 8, 6, 0, 0, 5, 6, 3, 1, 0, 0, 0, 2, 0, 1, 6, 2, 0, 1, 4, 4, 3, 7, 1, 8, 1, 1, 4, 1, 2, 3, 1, 0, 3, 3, 5, 6, 7, 4, 6, 3, 1, 0, 1, 5, 7, 3, 0, 0, 0, 0, 1, 0, 3, 6, 4, 0, 2, 8, 2, 7, 3, 7, 0, 8, 0, 1, 1, 5, 6, 1, 5, 4, 3, 9, 2, 2, 0, 6, 5, 6, 0, 1, 9, 9, 3, 7, 5, 0, 3, 9, 0, 0, 0, 0, 1, 4, 6, 6, 4, 6, 3, 1, 0, 9, 9, 1, 3, 4, 0, 4, 0, 1, 2, 0, 6, 0, 1, 8, 3, 4, 5, 0, 7, 0, 9, 6, 6, 8, 1, 0, 4, 1, 7, 1, 5, 3, 1, 4, 0, 1, 6, 1, 0, 2, 0, 1, 5, 7, 7, 2, 0, 1, 5, 0, 1, 0, 8, 0, 0, 9, 8, 2, 1, 0, 0, 1, 8, 0, 0, 7, 0, 4, 5, 0, 2, 4, 0, 8, 0, 1, 0, 2, 7, 1, 3, 6, 0, 8, 2, 0, 1, 3, 4, 3, 0, 1, 6, 9, 5, 4, 0, 0, 7, 6, 0, 3, 5, 5, 1, 2, 1, 0, 0, 5, 0, 0, 1, 2, 9, 1, 0, 0, 0, 0, 0, 1, 1, 0, 2, 9, 0, 0, 7, 9, 1, 0, 0, 4, 4, 4, 0, 0, 0, 0, 1, 0, 5, 3, 1, 0, 3, 0, 0, 0, 2, 1, 2, 0, 0, 4, 0, 0, 8, 3, 8, 2, 4, 4, 1, 7, 6, 0, 3, 6, 8, 0, 1, 0, 0, 0, 3, 0, 0, 9, 2, 0, 0, 3, 8, 2, 2, 3, 3, 6, 5, 0, 0, 3, 3, 0, 5, 6, 4, 3, 4, 6, 8, 0, 0, 6, 6, 5, 8, 6, 5, 4, 5, 8, 8, 0, 8, 0, 3, 7, 9, 0, 4, 9, 6, 7, 7, 0, 6, 0, 0, 2, 7, 1, 0, 5, 0, 0, 0, 3, 1, 0, 5, 0, 3, 1, 0, 0, 5, 0, 0, 3, 2, 0, 0, 6, 1, 2, 1, 5, 9, 8, 0, 3, 6, 5, 6, 5, 0, 0, 2, 9, 0, 2, 2, 8, 2, 9, 6, 1, 1, 8, 0, 0, 2, 8, 8, 3, 0, 1, 8, 2, 0, 0, 3, 2, 0, 0, 0, 9, 0, 8, 0, 0, 0, 0, 3, 1, 1, 4, 6, 0, 7, 1, 7, 0, 3, 0, 1, 0, 6, 5, 0, 0, 8, 1, 9, 0, 2, 9, 9, 4, 0, 2, 5, 4, 8, 3, 7, 2, 0, 5, 2, 3, 0, 0, 1, 8, 0, 5, 6, 0, 1, 0, 7, 4, 5, 9, 1, 1, 1, 0, 8, 3, 0, 3, 0, 3, 0, 3, 1, 5, 2, 0, 0, 6, 4, 1, 4, 4, 3, 7, 6, 4, 3, 0, 4, 0, 0, 7, 3, 1, 0, 0, 3, 5, 3, 1, 1, 7, 1, 9, 0, 4, 2, 5, 0, 8, 4, 0, 2, 5, 7, 5, 1, 1, 5, 1, 6, 2, 1, 1, 8, 0, 2, 9, 1, 7, 6, 7, 0, 9, 2, 6, 3, 3, 0, 0, 3, 8, 8, 0, 3, 0, 0, 2, 0, 3, 0, 3, 2, 2, 0, 0, 4, 6, 0, 1, 3, 0, 3, 2, 1, 6, 6, 0, 1, 1, 5, 7, 0, 2, 0, 1, 0, 4, 3, 0, 7, 0, 2, 2, 1, 0, 3, 8, 0, 0, 0, 9, 3, 0, 0, 7, 0, 1, 0, 0, 1, 4, 3, 5, 2, 4, 1, 6, 0, 0, 4, 4, 3, 0, 7, 4, 6, 0, 4, 0, 0, 1, 8, 3, 0, 7, 1, 2, 0, 7, 0, 0, 1, 3, 7, 7, 0, 1, 0, 5, 4, 0, 3, 1, 5, 0, 0, 2, 8, 3, 0, 2, 8, 0, 6, 2, 6, 6, 1, 9, 4, 0, 8, 3, 2, 4, 0, 0, 0, 5, 6, 1, 2, 8, 9, 6, 2, 0, 1, 7, 0, 8, 0, 0, 9, 8, 9, 8, 2, 0, 6, 3, 0, 0, 1, 0, 4, 0, 1, 3, 0, 5, 8, 5, 9, 4, 0, 0, 8, 6, 8, 9, 7, 4, 0, 0, 1, 8, 2, 7, 1, 7, 0, 0, 0, 0, 0, 1, 0, 8, 5, 4, 2, 9, 2, 1, 3, 0, 0, 0, 3, 2, 0, 8, 0, 0, 0, 1, 0, 8, 8, 2, 8, 0, 2, 2, 0, 2, 7, 7, 8, 6, 1, 0, 0, 2, 7, 5, 6, 0, 0, 3, 3, 8, 4, 7, 1, 2, 1, 2, 1, 8, 0, 5, 4, 0, 4, 1, 7, 0, 3, 2, 0, 2, 4, 1, 0, 9, 5, 0, 3, 6, 5, 1, 1, 0, 0, 0, 8, 4, 4, 8, 4, 9, 5, 2, 5, 0, 7, 1, 8, 7, 2, 0, 0, 9, 2, 0, 0, 5, 5, 2, 3, 1, 2, 0, 8, 1, 5, 0, 8, 0, 2, 4, 0, 8, 3, 1, 2, 8, 6, 0, 7, 4, 2, 2, 1, 0, 0, 0, 0, 7, 4, 0, 0, 0, 0, 1, 0, 3, 0, 0, 3, 2, 2, 1, 0, 4, 5, 2, 8, 3, 0, 9, 0, 0, 4, 0, 8, 0, 0, 0, 0, 0, 9, 4, 3, 1, 1, 8, 9, 0, 0, 5, 6, 5, 5, 0, 1, 2, 0, 1, 5, 0, 9, 0, 8, 2, 1, 0, 6, 2, 2, 0, 5, 3, 1, 2, 9, 2, 3, 3, 0, 8, 0, 0, 0, 0, 9, 5, 2, 0, 1, 0, 0, 9, 3, 1, 0, 6, 0, 0, 8, 0, 0, 1, 0, 6, 0, 0, 0, 8, 0, 1, 1, 5, 0, 2, 1, 0, 1, 0, 0, 0, 0, 2, 3, 0, 4, 9, 6, 0, 4, 1, 1, 1, 5, 5, 2, 8, 8, 1, 2, 4, 4, 0, 1, 0, 2, 4, 3, 1, 1, 1, 9, 2, 5, 5, 1, 0, 0, 9, 0, 8, 6, 0, 0, 0, 1, 4, 6, 4, 0, 5, 7, 2, 3, 8, 0, 6, 0, 7, 1, 9, 0, 0, 1, 3, 8, 0, 0, 0, 2, 9, 0, 4, 6, 2, 2, 0, 1, 2, 5, 0, 1, 0, 0, 0, 2, 2, 0, 0, 2, 9, 0, 0, 5, 2, 5, 0, 2, 0, 4, 1, 3, 7, 5, 1, 9, 0, 6, 0, 2, 0, 2, 1, 9, 5, 2, 9, 0, 0, 5, 0, 2, 0, 1, 0, 0, 0, 7, 8, 0, 0, 8, 3, 9, 0, 1, 5, 7, 3, 4, 0, 7, 0, 3, 0, 0, 5, 8, 2, 2, 0, 6, 4, 8, 7, 0, 8, 0, 0, 6, 5, 0, 2, 3, 0, 8, 1, 2, 3, 0, 6, 5, 3, 0, 9, 2, 9, 6, 8, 1, 0, 0, 3, 7, 2, 6, 1, 7, 6, 6, 9, 3, 9, 7, 5, 0, 2, 5, 8, 0, 0, 3, 8, 1, 4, 0, 5, 0, 3, 9, 2, 1, 2, 0, 6, 1, 8, 0, 0, 0, 2, 2, 9, 5, 3, 4, 5, 3, 0, 9, 3, 0, 1, 0, 0, 4, 1, 0, 8, 0, 2, 3, 1, 2, 8, 7, 4, 2, 1, 8, 8, 8, 1, 8, 1, 0, 0, 6, 0, 5, 8, 0, 0, 9, 0, 2, 2, 1, 0, 6, 0, 2, 0, 1, 4, 0, 0, 0, 2, 2, 9, 9, 1, 2, 0, 2, 4, 2, 0, 1, 0, 6, 3, 3, 3, 0, 2, 3, 0, 0, 2, 8, 9, 6, 5, 4, 5, 3, 2, 2, 0, 9, 7, 7, 4, 0, 8, 1, 1, 8, 8, 2, 0, 1, 7, 7, 4, 0, 3, 0, 8, 6, 5, 0, 7, 0, 1, 6, 0, 2, 6, 0, 0, 7, 7, 1, 3, 5, 0, 4, 1, 5, 8, 9, 1, 8, 2, 3, 2, 4, 0, 0, 3, 1, 0, 4, 2, 4, 0, 5, 1, 4, 1, 9, 0, 0, 7, 6, 7, 7, 7, 8, 0, 5, 4, 1, 0, 5, 3, 0, 0, 6, 2, 0, 8, 1, 0, 4, 5, 1, 4, 0, 9, 5, 0, 8, 4, 3, 3, 2, 3, 8, 0, 0, 1, 1, 0, 0, 2, 6, 2, 0, 5, 0, 4, 1, 6, 7, 1, 2, 3, 8, 5, 6, 3, 0, 8, 7, 4, 6, 0, 8, 6, 3, 0, 8, 0, 0, 1, 0, 5, 1, 0, 0, 1, 7, 4, 8, 5, 4, 4, 9, 7, 1, 4, 2, 0, 8, 0, 1, 5, 9, 0, 0, 5, 1, 9, 4, 2, 9, 5, 4, 7, 2, 9, 6, 0, 6, 1, 0, 3, 5, 0, 8, 7, 9, 0, 2, 1, 9, 0, 0, 1, 6, 1, 8, 5, 0, 0, 9, 3, 0, 1, 0, 3, 0, 9, 3, 7, 1, 1, 0, 7, 7, 9, 4, 0, 1, 7, 1, 1, 0, 1, 2, 1, 0, 9, 1, 1, 7, 7, 4, 0, 7, 7, 0, 0, 0, 9, 9, 0, 2, 0, 0, 8, 8, 0, 6, 5, 0, 2, 2, 0, 5, 1, 3, 1, 1, 4, 0, 0, 1, 0, 8, 0, 0, 6, 0, 1, 7, 8, 8, 3, 2, 8, 3, 3, 2, 0, 0, 0, 0, 2, 1, 0, 0, 9, 8, 3, 0, 6, 4, 0, 6, 5, 0, 7, 1, 0, 0, 3, 1, 2, 0, 5, 6, 0, 3, 0, 1, 3, 0, 0, 0, 0, 0, 5, 5, 1, 7, 0, 3, 9, 0, 1, 9, 0, 0, 8, 0, 0, 0, 1, 2, 1, 1, 1, 7, 2, 3, 0, 0, 1, 6, 0, 4, 0, 1, 2, 2, 6, 8, 1, 7, 4, 3, 5, 4, 2, 6, 6, 6, 0, 4, 9, 0, 4, 0, 6, 5, 2, 6, 0, 0, 9, 8, 6, 2, 1, 1, 2, 0, 1, 0, 0, 3, 0, 7, 6, 4, 0, 5, 2, 6, 5, 0, 4, 0, 3, 4, 8, 3, 0, 7, 0, 0, 0, 1, 0, 0, 1, 7, 3, 5, 0, 0, 5, 7, 4, 0, 2, 0, 9, 6, 7, 5, 2, 0, 4, 8, 9, 4, 0, 0, 2, 0, 0, 5, 7, 3, 0, 7, 0, 3, 0, 0, 1, 6, 6, 1, 0, 4, 5, 5, 7, 0, 1, 0, 1, 9, 3, 1, 0, 4, 3, 4, 0, 0, 0, 8, 6, 1, 7, 1, 2, 0, 0, 1, 3, 0, 1, 0, 2, 3, 7, 7, 4, 6, 9, 3, 2, 6, 0, 9, 0, 5, 6, 2, 0, 0, 8, 0, 0, 2, 2, 5, 3, 0, 0, 8, 5, 5, 8, 0, 4, 1, 6, 0, 1, 0, 0, 0, 2, 7, 4, 1, 1, 0, 4, 0, 6, 6, 0, 4, 4, 7, 0, 0, 0, 5, 0, 5, 1, 2, 2, 0, 0, 3, 7, 1, 8, 1, 1, 0, 3, 9, 2, 7, 5, 1, 0, 8, 0, 1, 4, 0, 0, 0, 4, 1, 7, 0, 1, 0, 4, 8, 1, 7, 0, 3, 4, 6, 1, 1, 1, 0, 2, 6, 5, 3, 4, 2, 0, 0, 1, 2, 0, 0, 7, 3, 4, 1, 5, 5, 2, 1, 5, 3, 0, 3, 0, 7, 5, 5, 5, 8, 1, 8, 1, 1, 9, 8, 0, 2, 0, 8, 0, 0, 2, 4, 6, 5, 0, 0, 7, 2, 6, 2, 3, 1, 0, 6, 9, 0, 7, 6, 2, 0, 0, 0, 5, 2, 2, 0, 3, 4, 2, 3, 2, 7, 0, 8, 0, 4, 6, 7, 5, 5, 8, 2, 2, 2, 2, 0, 1, 1, 9, 0, 0, 0, 2, 3, 6, 0, 3, 2, 0, 2, 2, 0, 0, 5, 0, 5, 9, 1, 6, 3, 0, 2, 6, 0, 0, 2, 1, 0, 7, 5, 5, 0, 3, 7, 2, 0, 3, 0, 7, 4, 1, 2, 3, 2, 6, 1, 7, 4, 4, 2, 0, 2, 5, 2, 0, 0, 1, 0, 0, 5, 1, 0, 1, 0, 0, 3, 2, 8, 4, 9, 0, 0, 4, 5, 8, 4, 8, 0, 3, 4, 3, 2, 4, 5, 2, 2, 1, 3, 0, 2, 9, 2, 1, 5, 1, 0, 0, 0, 2, 0, 1, 6, 3, 3, 6, 0, 3, 2, 2, 0, 0, 0, 4, 0, 0, 0, 3, 1, 0, 7, 3, 0, 0, 6, 0, 5, 8, 9, 8, 7, 0, 5, 1, 0, 0, 4, 0, 4, 3, 6, 0, 5, 0, 0, 4, 8, 5, 4, 0, 9, 3, 6, 4, 0, 3, 2, 0, 8, 0, 8, 0, 2, 0, 2, 0, 0, 5, 4, 0, 5, 5, 0, 3, 8, 5, 0, 0, 5, 9, 6, 2, 9, 6, 3, 0, 0, 5, 1, 2, 5, 1, 7, 8, 1, 0, 2, 6, 7, 0, 4, 0, 1, 0, 2, 3, 5, 0, 1, 0, 0, 1, 0, 0, 2, 0, 0, 0, 8, 8, 2, 2, 2, 0, 1, 1, 4, 8, 7, 5, 1, 0, 9, 2, 7, 4, 9, 0, 0, 0, 3, 0, 4, 0, 0, 2, 2, 4, 3, 7, 0, 3, 3, 1, 1, 1, 8, 0, 1, 0, 3, 0, 4, 0, 1, 3, 0, 0, 0, 5, 9, 3, 2, 0, 2, 0, 0, 0, 2, 3, 3, 7, 9, 8, 7, 0, 0, 0, 3, 8, 1, 1, 0, 4, 1, 0, 7, 0, 0, 8, 9, 5, 2, 4, 0, 0, 8, 8, 4, 0, 1, 7, 3, 7, 6, 1, 3, 1, 7, 7, 6, 1, 6, 0, 0, 3, 7, 0, 0, 0, 4, 3, 9, 8, 1, 2, 7, 3, 4, 0, 1, 8, 7, 7, 0, 9, 9, 9, 7, 4, 7, 4, 2, 0, 6, 2, 0, 0, 0, 4, 9, 0, 2, 0, 8, 9, 3, 3, 0, 6, 2, 0, 0, 8, 1, 1, 8, 5, 5, 2, 0, 0, 6, 0, 7, 0, 6, 6, 2, 0, 3, 0, 0, 0, 2, 7, 0, 6, 0, 8, 0, 8, 9, 3, 4, 2, 0, 7, 0, 0, 0, 4, 1, 1, 1, 0, 3, 0, 0, 6, 0, 6, 3, 2, 4, 1, 0, 8, 2, 4, 0, 0, 9, 6, 7, 0, 3, 0, 6, 0, 1, 7, 1, 5, 2, 0, 0, 8, 1, 1, 2, 1, 6, 6, 5, 2, 1, 7, 9, 0, 1, 1, 0, 0, 6, 5, 5, 0, 5, 2, 0, 4, 1, 3, 3, 1, 8, 0, 2, 8, 5, 0, 0, 5, 5, 1, 9, 0, 1, 9, 0, 1, 0, 3, 1, 0, 8, 0, 0, 1, 5, 4, 0, 8, 0, 2, 0, 2, 3, 6, 0, 2, 8, 0, 3, 7, 0, 4, 1, 0, 9, 9, 5, 1, 1, 6, 0, 4, 3, 0, 3, 2, 5, 6, 2, 3, 0, 0, 4, 1, 0, 0, 9, 0, 5, 7, 2, 0, 9, 9, 0, 0, 3, 0, 1, 2, 6, 4, 5, 2, 8, 3, 0, 0, 3, 6, 6, 7, 4, 3, 5, 0, 6, 2, 0, 2, 0, 1, 2, 0, 2, 5, 0, 0, 0, 0, 9, 1, 4, 6, 7, 9, 1, 4, 0, 1, 1, 6, 0, 9, 9, 8, 0, 1, 1, 1, 5, 9, 6, 1, 1, 3, 7, 0, 0, 2, 1, 0, 3, 8, 0, 3, 6, 0, 2, 4, 1, 6, 1, 8, 0, 5, 0, 8, 6, 6, 2, 6, 3, 0, 8, 3, 4, 9, 5, 9, 4, 2, 1, 3, 4, 1, 5, 0, 5, 9, 5, 1, 3, 3, 4, 7, 5, 0, 6, 4, 0, 5, 3, 2, 0, 0, 1, 7, 1, 0, 7, 1, 3, 0, 5, 2, 2, 2, 3, 9, 2, 4, 1, 9, 7, 4, 0, 4, 6, 9, 0, 7, 2, 6, 0, 2, 1, 2, 3, 0, 6, 4, 0, 0, 0, 9, 0, 0, 3, 0, 0, 2, 5, 1, 0, 2, 4, 3, 7, 4, 7, 7, 5, 5, 8, 9, 0, 0, 1, 2, 2, 1, 0, 0, 0, 0, 0, 5, 1, 7, 5, 3, 1, 7, 3, 0, 0, 4, 2, 0, 4, 0, 4, 1, 9, 0, 8, 2, 2, 6, 0, 3, 9, 7, 1, 1, 9, 0, 0, 2, 0, 0, 3, 3, 1, 6, 8, 3, 0, 1, 0, 0, 0, 1, 0, 7, 0, 0, 2, 0, 1, 1, 6, 1, 2, 7, 9, 6, 0, 7, 1, 3, 7, 0, 1, 8, 7, 1, 5, 0, 8, 6, 1, 0, 0, 4, 0, 9, 1, 1, 1, 0, 8, 3, 4, 0, 9, 4, 1, 7, 3, 0, 2, 2, 8, 0, 8, 1, 1, 1, 5, 3, 6, 2, 0, 6, 6, 2, 4, 9, 7, 8, 5, 0, 0, 2, 2, 2, 8, 2, 5, 8, 8, 0, 4, 0, 3, 0, 4, 0, 1, 0, 7, 7, 1, 0, 5, 2, 0, 7, 0, 0, 2, 3, 1, 3, 1, 0, 9, 1, 2, 4, 8, 2, 0, 4, 4, 8, 9, 2, 0, 5, 0, 1, 0, 0, 4, 0, 0, 4, 7, 3, 1, 6, 2, 1, 0, 1, 8, 7, 2, 7, 5, 3, 8, 0, 9, 0, 4, 9, 6, 3, 0, 6, 2, 3, 2, 8, 8, 6, 0, 5, 9, 1, 8, 0, 0, 0, 0, 6, 3, 0, 0, 4, 9, 7, 0, 2, 4, 4, 1, 9, 8, 3, 0, 1, 0, 0, 8, 0, 8, 8, 4, 9, 2, 0, 6, 2, 2, 2, 4, 2, 7, 4, 8, 8, 6, 3, 4, 0, 8, 0, 3, 8, 5, 1, 0, 0, 0, 4, 8, 0, 6, 0, 0, 0, 0, 1, 1, 0, 4, 6, 3, 7, 0, 4, 1, 0, 1, 6, 0, 0, 0, 3, 5, 0, 0, 4, 0, 0, 0, 7, 4, 1, 0, 4, 4, 0, 1, 0, 5, 9, 0, 0, 4, 4, 0, 7, 0, 0, 2, 2, 5, 0, 0, 0, 3, 0, 0, 0, 5, 1, 4, 7, 9, 1, 0, 2, 4, 9, 5, 0, 6, 2, 1, 0, 0, 6, 0, 1, 0, 0, 0, 0, 6, 4, 4, 1, 0, 5, 0, 0, 0, 2, 9, 5, 2, 6, 2, 0, 2, 0, 0, 0, 0, 0, 0, 0, 6, 3, 2, 7, 0, 8, 0, 1, 4, 1, 0, 0, 0, 6, 0, 5, 0, 2, 5, 7, 0, 2, 9, 0, 5, 7, 0, 0, 9, 8, 4, 2, 6, 8, 3, 7, 0, 1, 4, 5, 7, 0, 8, 8, 6, 2, 0, 1, 7, 3, 0, 4, 2, 1, 0, 0, 1, 7, 0, 0, 4, 4, 0, 2};

DCAwarePolicy::DCAwarePolicy(const String& local_dc, size_t used_hosts_per_remote_dc,
                             bool skip_remote_dcs_for_local_cl)
    : local_dc_(local_dc)
    , used_hosts_per_remote_dc_(used_hosts_per_remote_dc)
    , skip_remote_dcs_for_local_cl_(skip_remote_dcs_for_local_cl)
    , local_dc_live_hosts_(new HostVec())
    , index_(0) {
  uv_rwlock_init(&available_rwlock_);
  if (used_hosts_per_remote_dc_ > 0 || !skip_remote_dcs_for_local_cl) {
    LOG_WARN("Remote multi-dc settings have been deprecated and will be removed"
             " in the next major release");
  }
}

DCAwarePolicy::~DCAwarePolicy() { uv_rwlock_destroy(&available_rwlock_); }

void DCAwarePolicy::init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                         const String& local_dc) {
  if (local_dc_.empty()) { // Only override if no local DC was specified.
    local_dc_ = local_dc;
  }

  if (local_dc_.empty() && connected_host && !connected_host->dc().empty()) {
    LOG_INFO("Using '%s' for the local data center "
             "(if this is incorrect, please provide the correct data center)",
             connected_host->dc().c_str());
    local_dc_ = connected_host->dc();
  }

  available_.resize(hosts.size());
  std::transform(hosts.begin(), hosts.end(), std::inserter(available_, available_.begin()),
                 GetAddress());

  for (HostMap::const_iterator i = hosts.begin(), end = hosts.end(); i != end; ++i) {
    on_host_added(i->second);
  }
  if (random != NULL) {
    index_ = random->next(std::max(static_cast<size_t>(1), hosts.size()));
  }
}

CassHostDistance DCAwarePolicy::distance(const Host::Ptr& host) const {
  if (local_dc_.empty() || host->dc() == local_dc_) {
    return CASS_HOST_DISTANCE_LOCAL;
  }

  const CopyOnWriteHostVec& hosts = per_remote_dc_live_hosts_.get_hosts(host->dc());
  size_t num_hosts = std::min(hosts->size(), used_hosts_per_remote_dc_);
  for (size_t i = 0; i < num_hosts; ++i) {
    if ((*hosts)[i]->address() == host->address()) {
      return CASS_HOST_DISTANCE_REMOTE;
    }
  }

  return CASS_HOST_DISTANCE_IGNORE;
}

QueryPlan* DCAwarePolicy::new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                         const TokenMap* token_map) {
  CassConsistency cl =
      request_handler != NULL ? request_handler->consistency() : CASS_DEFAULT_CONSISTENCY;
  return new DCAwareQueryPlan(this, cl, index_++);
}

bool DCAwarePolicy::is_host_up(const Address& address) const {
  ScopedReadLock rl(&available_rwlock_);
  return available_.count(address) > 0;
}

void DCAwarePolicy::on_host_added(const Host::Ptr& host) {
  const String& dc = host->dc();
  if (local_dc_.empty() && !dc.empty()) {
    LOG_INFO("Using '%s' for local data center "
             "(if this is incorrect, please provide the correct data center)",
             host->dc().c_str());
    local_dc_ = dc;
  }

  if (dc == local_dc_) {
    add_host(local_dc_live_hosts_, host);
  } else {
    per_remote_dc_live_hosts_.add_host_to_dc(dc, host);
  }
}

void DCAwarePolicy::on_host_removed(const Host::Ptr& host) {
  const String& dc = host->dc();
  if (dc == local_dc_) {
    remove_host(local_dc_live_hosts_, host);
  } else {
    per_remote_dc_live_hosts_.remove_host_from_dc(host->dc(), host);
  }

  ScopedWriteLock wl(&available_rwlock_);
  available_.erase(host->address());
}

void DCAwarePolicy::on_host_up(const Host::Ptr& host) {
  on_host_added(host);

  ScopedWriteLock wl(&available_rwlock_);
  available_.insert(host->address());
}

void DCAwarePolicy::on_host_down(const Address& address) {
  if (!remove_host(local_dc_live_hosts_, address) &&
      !per_remote_dc_live_hosts_.remove_host(address)) {
    LOG_DEBUG("Attempted to mark host %s as DOWN, but it doesn't exist",
              address.to_string().c_str());
  }

  ScopedWriteLock wl(&available_rwlock_);
  available_.erase(address);
}

bool DCAwarePolicy::skip_remote_dcs_for_local_cl() const {
  ScopedReadLock rl(&available_rwlock_);
  return skip_remote_dcs_for_local_cl_;
}

size_t DCAwarePolicy::used_hosts_per_remote_dc() const {
  ScopedReadLock rl(&available_rwlock_);
  return used_hosts_per_remote_dc_;
}

const String& DCAwarePolicy::local_dc() const {
  ScopedReadLock rl(&available_rwlock_);
  return local_dc_;
}

void DCAwarePolicy::PerDCHostMap::add_host_to_dc(const String& dc, const Host::Ptr& host) {
  ScopedWriteLock wl(&rwlock_);
  Map::iterator i = map_.find(dc);
  if (i == map_.end()) {
    CopyOnWriteHostVec hosts(new HostVec());
    hosts->push_back(host);
    map_.insert(Map::value_type(dc, hosts));
  } else {
    add_host(i->second, host);
  }
}

void DCAwarePolicy::PerDCHostMap::remove_host_from_dc(const String& dc, const Host::Ptr& host) {
  ScopedWriteLock wl(&rwlock_);
  Map::iterator i = map_.find(dc);
  if (i != map_.end()) {
    core::remove_host(i->second, host);
  }
}

bool DCAwarePolicy::PerDCHostMap::remove_host(const Address& address) {
  ScopedWriteLock wl(&rwlock_);
  for (Map::iterator i = map_.begin(), end = map_.end(); i != end; ++i) {
    if (core::remove_host(i->second, address)) {
      return true;
    }
  }
  return false;
}

const CopyOnWriteHostVec& DCAwarePolicy::PerDCHostMap::get_hosts(const String& dc) const {
  ScopedReadLock rl(&rwlock_);
  Map::const_iterator i = map_.find(dc);
  if (i == map_.end()) return no_hosts_;

  return i->second;
}

void DCAwarePolicy::PerDCHostMap::copy_dcs(KeySet* dcs) const {
  ScopedReadLock rl(&rwlock_);
  for (Map::const_iterator i = map_.begin(), end = map_.end(); i != end; ++i) {
    dcs->insert(i->first);
  }
}

// Helper functions to prevent copy (Notice: "const CopyOnWriteHostVec&")

static const Host::Ptr& get_next_host(const CopyOnWriteHostVec& hosts, size_t index) {
  return (*hosts)[index % hosts->size()];
}

static const Host::Ptr& get_next_host_bounded(const CopyOnWriteHostVec& hosts, size_t index,
                                              size_t bound) {
  return (*hosts)[index % std::min(hosts->size(), bound)];
}

static size_t get_hosts_size(const CopyOnWriteHostVec& hosts) { return hosts->size(); }


int pickZipfian05(){
  int idx = generate_random(0, 9999);
  printf("rand %d\n", idx);
  return zipfian[idx];
}

DCAwarePolicy::DCAwareQueryPlan::DCAwareQueryPlan(const DCAwarePolicy* policy, CassConsistency cl,
                                                  size_t start_index)
    : policy_(policy)
    , cl_(cl)
    , hosts_(policy_->local_dc_live_hosts_)
    , local_remaining_(get_hosts_size(hosts_))
    , remote_remaining_(0)
    {
            index_ = pickZipfian05();
//	    int max = 1024;
//	    int min = 1;
//        int range = max - min + 1;
//        int num = rand() % range + min;
//        if (1 <= num < 2) {
//            index_ = 0;
//        } else if (2 <= num < 4) {
//        	index_ = 1;
//        } else if (4 <= num < 8) {
//        	index_ = 2;
//        } else if (8 <= num < 16) {
//        	index_ = 3;
//        } else if (16 <= num < 32) {
//        	index_ = 4;
//        } else if (32 <= num < 64) {
//        	index_ = 5;
//        } else if (64 <= num < 128) {
//        	index_ = 6;
//        } else if (128 <= num < 256) {
//        	index_ = 7;
//        } else if (256 <= num < 512) {
//        	index_ = 8;
//        } else {
//        	index_ = 9;
//        }
        server_index = index_;
    }

Host::Ptr DCAwarePolicy::DCAwareQueryPlan::compute_next() {
  while (local_remaining_ > 0) {
    --local_remaining_;
    const Host::Ptr& host(get_next_host(hosts_, index_++));
    if (policy_->is_host_up(host->address())) {
      return host;
    }
  }

  if (policy_->skip_remote_dcs_for_local_cl_ && is_dc_local(cl_)) {
    return Host::Ptr();
  }

  if (!remote_dcs_) {
    remote_dcs_.reset(new PerDCHostMap::KeySet());
    policy_->per_remote_dc_live_hosts_.copy_dcs(remote_dcs_.get());
  }

  while (true) {
    while (remote_remaining_ > 0) {
      --remote_remaining_;
      const Host::Ptr& host(
          get_next_host_bounded(hosts_, index_++, policy_->used_hosts_per_remote_dc_));
      if (policy_->is_host_up(host->address())) {
        return host;
      }
    }

    if (remote_dcs_->empty()) {
      break;
    }

    PerDCHostMap::KeySet::iterator i = remote_dcs_->begin();
    hosts_ = policy_->per_remote_dc_live_hosts_.get_hosts(*i);
    remote_remaining_ = std::min(get_hosts_size(hosts_), policy_->used_hosts_per_remote_dc_);
    remote_dcs_->erase(i);
  }

  return Host::Ptr();
}
