/*
 * File:   keygen.h
 * Author: t35brown
 *
 * Created on August 5, 2019, 7:24 PM
 */

#ifndef KEYGEN_H
#define KEYGEN_H

#include <algorithm>

#include <cassert>
#include "plaf.h"

template <typename K>
class KeyGeneratorUniform {
private:
    PAD;
    Random64 * rng;
    int maxKey;
    PAD;
public:
    KeyGeneratorUniform(Random64 * _rng, int _maxKey) : rng(_rng), maxKey(_maxKey) {}
    K next() {
        auto result = 1+rng->next(maxKey);
        assert((result >= 1) && (result <= maxKey));
        // GSTATS_ADD_IX(tid, key_gen_histogram, 1, result);
        return result;
    }
};

class KeyGeneratorZipfData {
public:
    PAD;
    int maxKey;
    double c = 0; // Normalization constant
    double* sum_probs; // Pre-calculated sum of probabilities
    PAD;
    KeyGeneratorZipfData(const int _maxKey, const double _alpha) {
        maxKey = _maxKey;
        // Compute normalization constant c for implied key range: [1, maxKey]
        for (int i = 1; i <= _maxKey; i++) {
            c += ((double)1) / pow((double) i, _alpha);
        }
        double* probs = new double[_maxKey+1];
        for (int i = 1; i <= _maxKey; i++) {
            probs[i] = (((double)1) / pow((double) i, _alpha)) / c;
        }
        // Random should be seeded already (in main)
        std::random_shuffle(probs + 1, probs + maxKey);
        sum_probs = new double[_maxKey+1];
        sum_probs[0] = 0;
        for (int i = 1; i <= _maxKey; i++) {
            sum_probs[i] = sum_probs[i - 1] + probs[i];
        }

        delete[] probs;
    }
    ~KeyGeneratorZipfData() {
        delete[] sum_probs;
    }
};
template <typename K>
class KeyGeneratorZipf {
private:
    PAD;
    KeyGeneratorZipfData * data;
    Random64 * rng;
    PAD;
public:
    KeyGeneratorZipf(KeyGeneratorZipfData * _data, Random64 * _rng)
          : data(_data), rng(_rng) {}
    K next() {
        double z; // Uniform random number (0 < z < 1)
        int zipf_value = 0; // Computed exponential value to be returned
        // Pull a uniform random number (0 < z < 1)
        do {
            z = (rng->next() / (double) std::numeric_limits<uint64_t>::max());
        } while ((z == 0) || (z == 1));
        zipf_value = std::upper_bound(data->sum_probs + 1, data->sum_probs + data->maxKey + 1, z) - data->sum_probs;
        // Assert that zipf_value is between 1 and N
        assert((zipf_value >= 1) && (zipf_value <= data->maxKey));
        // GSTATS_ADD_IX(tid, key_gen_histogram, 1, zipf_value);
        return (zipf_value);
    }
};

// class KeyGeneratorZipfData {
// public:
//     PAD;
//     int maxKey;
//     double theta;
//     double c = 0; // Normalization constant
//     double * sum_probs; // Pre-calculated sum of probabilities
//     PAD;

//     KeyGeneratorZipfData(int _maxKey, double _alpha) {
// //        std::cout<<"start KeyGeneratorZipfData"<<std::endl;
//         maxKey = _maxKey;
//         theta = _alpha;

//         // Compute normalization constant c for implied key range: [1, maxKey]
//         int i;
//         for (i = 1; i <= _maxKey; i++)
//             c = c + (1.0 / pow((double) i, theta));
//         c = 1.0 / c;

//         sum_probs = new double[_maxKey+1];
//         sum_probs[0] = 0;
//         for (i = 1; i <= _maxKey; i++) {
//             sum_probs[i] = sum_probs[i - 1] + c / pow((double) i, theta);
//         }
// //        std::cout<<"end KeyGeneratorZipfData"<<std::endl;
//     }
//     ~KeyGeneratorZipfData() {
//         delete[] sum_probs;
//     }
// };

// template <typename K>
// class KeyGeneratorZipf {
// private:
//     PAD;
//     KeyGeneratorZipfData * data;
//     Random64 * rng;
//     PAD;
// public:
//     KeyGeneratorZipf(KeyGeneratorZipfData * _data, Random64 * _rng)
//           : data(_data), rng(_rng) {}
//     K next() {
//         double z; // Uniform random number (0 < z < 1)
//         int zipf_value = 0; // Computed exponential value to be returned
//         int low, high, mid; // Binary-search bounds

//         // Pull a uniform random number (0 < z < 1)
//         do {
//             z = (rng->next() / (double) std::numeric_limits<uint64_t>::max());
// //            printf("    z=%lf\n", z);
//         } while ((z == 0) || (z == 1));

//         // Map z to the value
//         low = 1, high = data->maxKey;
//         do {
//             mid = floor((low + high) / 2);
//             if (data->sum_probs[mid] >= z && data->sum_probs[mid - 1] < z) {
//                 zipf_value = mid;
//                 break;
//             } else if (data->sum_probs[mid] >= z) {
//                 high = mid - 1;
//             } else {
//                 low = mid + 1;
//             }
//         } while (low <= high);

//         // Assert that zipf_value is between 1 and N
//         assert((zipf_value >= 1) && (zipf_value <= data->maxKey));

//         GSTATS_ADD_IX(tid, key_gen_histogram, 1, zipf_value);
//         return (zipf_value);
//     }
// };

// Sampler taken from https://commons.apache.org/proper/commons-math/apidocs/src-html/org/apache/commons/math4/distribution/ZipfDistribution.html#line.44
// Paper: Rejection-Inversion to Generate Variates from Monotone Discrete Distributions.
struct ZipfRejectionInversionSamplerData {
    int* mapping;
    const int maxkey;
    ZipfRejectionInversionSamplerData(int _maxkey): maxkey(_maxkey) {
        mapping = new int[maxkey + 1];
        #pragma omp parallel for
        for (int i = 0; i < maxkey + 1; ++i) {
            mapping[i] = i;
        }
        std::random_shuffle(mapping + 1, mapping + maxkey);
    }

    ~ZipfRejectionInversionSamplerData() {
        delete[] mapping;
    }
};


class ZipfRejectionInversionSampler {
    const double exponent;
    const int maxkey;
    Random64* rng;
    ZipfRejectionInversionSamplerData* const data;
    double hIntegralX1;
    double hIntegralmaxkey;
    double s;

    double hIntegral(const double x) {
        return helper2((1 - exponent) * log(x)) * log(x);
    }

    double h(const double x) {
        return exp(-exponent * log(x));
    }

    double hIntegralInverse(const double x) {
        double t = x * (1 - exponent);
        if (t < -1) {
            // Limit value to the range [-1, +inf).
            // t could be smaller than -1 in some rare cases due to numerical errors.
            t = -1;
        }
        return exp(helper1(t) * x);
    }

    double helper1(const double x) {
        // if (abs(x)>1e-8) {
            return log(x + 1)/x;
        // }
        // else {
        //     return 1.-x*((1./2.)-x*((1./3.)-x*(1./4.)));
        // }
    }

    double helper2(const double x) {
        // if (FastMath.abs(x)>1e-8) {
            return (exp(x) - 1)/x;
        // }
        // else {
        //     return 1.+x*(1./2.)*(1.+x*(1./3.)*(1.+x*(1./4.)));
        // }
    }

public:
    /** Simple constructor.
     * @param maxkey number of elements
     * @param exponent exponent parameter of the distribution
     */
    ZipfRejectionInversionSampler(ZipfRejectionInversionSamplerData* const _data, const double _exponent, Random64 * _rng): data(_data), maxkey(_data->maxkey), exponent(_exponent), rng(_rng) {
        if (exponent <= 1) {
            std::cout << "-dist-zipf-fast only works with exponents greater than 1." << std::endl;
            exit(-1);
        }
        hIntegralX1 = hIntegral(1.5) - 1;
        hIntegralmaxkey = hIntegral(maxkey + 0.5);
        s = 2 - hIntegralInverse(hIntegral(2.5) - h(2));
    }

    /** Generate one integral number in the range [1, maxkey].
     * @param random random generator to use
     * @return generated integral number in the range [1, maxkey]
     */
    int next() {
        while(true) {
            // Pull a uniform random number (0 < z < 1)
            const double z = (rng->next() / (double) std::numeric_limits<uint64_t>::max());
            const double u = hIntegralmaxkey + z * (hIntegralX1 - hIntegralmaxkey);
            // u is uniformly distributed in (hIntegralX1, hIntegralmaxkey]

            double x = hIntegralInverse(u);

            int k = (int)(x + 0.5);

            if (k < 1) {
                k = 1;
            }
            else if (k > maxkey) {
                k = maxkey;
            }

            if (k - x <= s || u >= hIntegral(k + 0.5) - h(k)) {
                return data->mapping[k];
            }
        }
    }

};

#endif /* KEYGEN_H */

