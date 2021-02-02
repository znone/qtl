#ifndef _APPLY_TUPLE_H_
#define _APPLY_TUPLE_H_

#include <stddef.h>
#include <tuple>
#include <type_traits>
#include <utility>

namespace qtl
{

namespace detail
{

#if ((defined(_MSVC_LANG) && _MSVC_LANG >= 201703L) || __cplusplus >= 201703L)

template <class F, class Tuple>
inline constexpr decltype(auto) apply_tuple(F&& f, Tuple&& t)
{
	return std::apply(std::forward<F>(f), std::forward<Tuple>(t));
}

#else

namespace detail
{
	template<size_t N>
	struct apply 
	{
		template<typename F, typename T, typename... A>
		static inline auto apply_tuple(F&& f, T&& t, A&&... a)
			-> decltype(apply<N-1>::apply_tuple(
			std::forward<F>(f), std::forward<T>(t),
			std::get<N-1>(std::forward<T>(t)), std::forward<A>(a)...
			))
		{
			return apply<N-1>::apply_tuple(std::forward<F>(f), std::forward<T>(t),
				std::get<N-1>(std::forward<T>(t)), std::forward<A>(a)...
				);
		}
	};

	template<>
	struct apply<0> 
	{
		template<typename F, typename T, typename... A>
		static inline typename std::result_of<F(A...)>::type apply_tuple(F&& f, T&&, A&&... a)
		{
			return std::forward<F>(f)(std::forward<A>(a)...);
		}
	};
}

template<typename F, typename T>
inline auto apply_tuple(F&& f, T&& t)
    -> decltype(detail::apply< std::tuple_size<
        typename std::decay<T>::type
    >::value>::apply_tuple(std::forward<F>(f), std::forward<T>(t)))
{
	return detail::apply< std::tuple_size<
        typename std::decay<T>::type
    >::value>::apply_tuple(std::forward<F>(f), std::forward<T>(t));
}

#endif // C++17

}

}

#endif //_APPLY_TUPLE_H_
