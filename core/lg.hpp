#pragma once

#ifdef USE_SHARED_LIVEGRAPH

#include "LiveGraph/bind/livegraph.hpp"
using namespace lg;

#else

#include "LiveGraph/core/livegraph.hpp"
using namespace livegraph;

#endif
