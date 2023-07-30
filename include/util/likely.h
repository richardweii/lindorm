#ifndef _LIKELY_H_
#define _LIKELY_H_

/**
 * Treat the condition as likely.
 *
 * @def LIKELY
 */
/**
 * Treat the condition as unlikely.
 *
 * @def UNLIKELY
 */
#if defined(__GNUC__)
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#endif