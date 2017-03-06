#ifndef _STDAFX_H_
#define _STDAFX_H_

#include <stdio.h>
#include <cpptest.h>

#ifdef _WIN32

#ifndef NOMINMAX
#define NOMINMAX 1
#endif //NOMINMAX

#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0501
#endif

#include <WinSock2.h>
#include <windows.h>

#endif

#endif //_STDAFX_H_
