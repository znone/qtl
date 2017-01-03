#ifndef _STDAFX_H_
#define _STDAFX_H_

#include <stdio.h>
#include <cpptest.h>

#ifdef _WIN32

#ifndef NOMINMAX
#define NOMINMAX 1
#endif //NOMINMAX

#ifndef _WIN32_WINNT		// 允许使用特定于 Windows XP 或更高版本的功能。
#define _WIN32_WINNT 0x0501	// 将此值更改为相应的值，以适用于 Windows 的其他版本。
#endif

#include <WinSock2.h>
#include <windows.h>

#endif

#endif //_STDAFX_H_
