#ifndef _TEST_SUITE_H_
#define _TEST_SUITE_H_

#include <exception>
#include <cpptest.h>

class TestSuite : public Test::Suite
{
protected:
	void assert_exception(const char* file, int line, std::exception& e)
	{
		assertment(Test::Source(file, line, e.what()));
	}
};

#define ASSERT_EXCEPTION(e) \
	assert_exception(__FILE__, __LINE__, e);

#endif //_TEST_SUITE_H_
