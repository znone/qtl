#ifndef _QTL_MINMAX_H_
#define _QTL_MINMAX_H_

namespace qtl
{

namespace detail
{

	template<class T, T... Is>
	struct max;

	template<class T, T I1, T... Is >
	struct max<T, I1, Is...> : public std::integral_constant<T, (I1 > max<T, Is...>::value ? I1 : max<T, Is...>::value)>
	{
	};
	template<class T, T I1>
	struct max<T, I1> : public std::integral_constant<T, I1>
	{
	};

	template<class T, T... Is>
	struct min;

	template<class T, T I1, T... Is>
	struct min<T, I1, Is...> : public std::integral_constant<T, (I1 < max<T, Is...>::value ? I1 : max<T, Is...>::value)>
	{
	};
	template<class T, T I1>
	struct min<T, I1> : public std::integral_constant<T, I1>
	{
	};
}

template<class T, T... I>
constexpr T max = detail::max<T, I...>::value;

template<class T, T... I>
constexpr T min = detail::min<T, I...>::value;

}

#endif //_QTL_MINMAX_H_

