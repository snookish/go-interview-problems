package main

import (
	"context"
)

type Getter interface {
	Get(ctx context.Context, address, key string) (string, error)
}

type GetterResponse struct {
	Err  error
	Data string
}

// Call `Getter.Get()` for each address in parallel.
// Returns the first successful response.
// If all requests fail, returns an error.
// func Get(ctx context.Context, getter Getter, addresses []string, key string) (string, error) {
// 	if len(addresses) == 0 {
// 		return "", nil
// 	}

// 	var errorsCount int
// 	responseChan := make(chan GetterResponse, len(addresses))

// 	for _, address := range addresses {
// 		go func() {
// 			resp, err := getter.Get(ctx, address, key)
// 			responseChan <- GetterResponse{Err: err, Data: resp}
// 		}()
// 	}

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return "", ctx.Err()
// 		case data := <-responseChan:
// 			if data.Err == nil {
// 				return data.Data, nil
// 			}

// 			errorsCount++
// 			if errorsCount == len(addresses) {
// 				return "", data.Err
// 			}
// 		}
// 	}
// }

func Get(ctx context.Context, getter Getter, addresses []string, key string) (string, error) {
	if len(addresses) == 0 {
		return "", nil
	}

	var errorsCount int
	errorCh := make(chan error)
	respCh := make(chan string, 1)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, address := range addresses {
		go func() {
			resp, err := getter.Get(ctx, address, key)
			if err != nil {
				select {
				case errorCh <- err:
					return
				case <-ctx.Done():
					return
				}
			}

			select {
			case respCh <- resp:
			default:
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case data := <-respCh:
			return data, nil
		case err := <-errorCh:
			errorsCount++
			if errorsCount == len(addresses) {
				return "", err
			}
		}
	}
}
