package main

import (
	"database/sql"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (:cl, :status, :address, :at)",
		sql.Named("cl", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("at", p.CreatedAt),
	)
	if err != nil {
		return -1, err
	}
	// верните идентификатор последней добавленной записи
	id, err := res.LastInsertId()
	if err != nil {
		return -1, err
	}
	return int(id), err
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	row := s.db.QueryRow("SELECT * FROM parcel WHERE number = :num", sql.Named("num", number))
	if row.Err() != nil {
		return Parcel{}, row.Err()
	}
	// заполните объект Parcel данными из таблицы
	p := Parcel{}

	if err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
		return Parcel{}, err
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	rows, err := s.db.Query("SELECT * FROM parcel WHERE client = :cl", sql.Named("cl", client))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// заполните срез Parcel данными из таблицы
	var res []Parcel

	for rows.Next() {
		p := Parcel{}

		err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			return nil, err
		}
		res = append(res, p)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel
	_, err := s.db.Exec("UPDATE parcel SET status = :status WHERE number = :num", sql.Named("status", status), sql.Named("num", number))

	return err
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	row := s.db.QueryRow("SELECT status FROM parcel WHERE number = :num", sql.Named("num", number))
	if row.Err() != nil {
		return row.Err()
	}

	status := ""
	err := row.Scan(&status)
	if err != nil {
		return err
	}

	if status != ParcelStatusRegistered {
		return nil
	}

	_, err = s.db.Exec("UPDATE parcel SET address = :address WHERE number = :num", sql.Named("address", address), sql.Named("num", number))

	return err
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	row := s.db.QueryRow("SELECT status FROM parcel WHERE number = :num", sql.Named("num", number))
	if row.Err() != nil {
		return row.Err()
	}

	status := ""
	err := row.Scan(&status)
	if err != nil {
		return err
	}

	if status != ParcelStatusRegistered {
		return nil
	}

	_, err = s.db.Exec("DELETE FROM parcel WHERE number = :num", sql.Named("num", number))

	return err
}
