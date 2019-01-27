package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection connection) {
		this.conn = connection;
	}
	
	@Override
	public void insert(Seller seller) {
		PreparedStatement st = null;
		
		try {
			st = conn.prepareStatement(
					"INSERT INTO seller (\r\n" + 
					"	Name, \r\n" + 
					"	Email, \r\n" + 
					"	BirthDate, \r\n" + 
					"	BaseSalary, \r\n" + 
					"	DepartmentId)\r\n" + 
					"VALUES(?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);
			
			st.setString(1, seller.getName());
			st.setString(2, seller.getEmail());
			st.setDate(3, new java.sql.Date(seller.getBirthDate().getTime()));
			st.setDouble(4,  seller.getBaseSalary());
			st.setInt(5, seller.getDepartment().getId());
			
			int rowsAffected = st.executeUpdate();
			
			if (rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					seller.setId(id);
				}
				DB.closeResultSet(rs);
			} else {
				throw new DbException("Problems to insert new Seller");
			}
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Seller seller) {
		PreparedStatement st = null;
		
		try {
			st = conn.prepareStatement(
					"UPDATE seller SET\r\n" + 
					"	Name = ?,\r\n" + 
					"	Email = ?, \r\n" + 
					"	BirthDate = ?, \r\n" + 
					"	BaseSalary = ?, \r\n" + 
					"	DepartmentId = ?\r\n" + 
					"WHERE Id = ?");
			
			st.setString(1,  seller.getName());
			st.setString(2, seller.getEmail());
			st.setDate(3, new java.sql.Date(seller.getBirthDate().getTime()));
			st.setDouble(4,  seller.getBaseSalary());
			st.setInt(5, seller.getDepartment().getId());
			st.setInt(6, seller.getId());
			
			int rowsAffected = st.executeUpdate();
			
			if (rowsAffected == 0) {
				throw new DbException("Seller Id = " + seller.getId() + " not found!");
			}
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
		
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
		
		try {
			st = conn.prepareStatement(
					"DELETE\r\n" + 
					"FROM seller\r\n" + 
					"WHERE Id = ?");
			
			st.setInt(1, id);
			
			int rowsAffected = st.executeUpdate();
			
			if (rowsAffected == 0) {
				throw new DbException("Seller Id = " + id + " not found!");
			}
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}
	
	@Override
	public Seller findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT\r\n" + 
					"   s.Id,\r\n" + 
					"	s.Name,\r\n" + 
					"	s.Email,\r\n" + 
					"	s.BirthDate,\r\n" + 
					"	s.BaseSalary,\r\n" + 
					"	d.Id As DepartmentId,\r\n" + 
					"	d.Name AS DepartmentName\r\n" + 
					"FROM seller s \r\n" + 
					"	INNER JOIN department d\r\n" + 
					"	ON s.DepartmentId = d.Id\r\n" + 
					"WHERE s.Id = ?");
			
			st.setInt(1, id);
			
			rs = st.executeQuery();
			
			if (rs.next()) {
				Department department = instantiateDepartment(rs);
				Seller seller = instantiateSeller(rs, department);
				return seller;
			}
			return null;
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}
	
	@Override
	public List<Seller> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					"SELECT\r\n" + 
					"	s.Id,\r\n" + 
					"	s.Name,\r\n" + 
					"	s.Email,\r\n" + 
					"	s.BirthDate,\r\n" + 
					"	s.BaseSalary,\r\n" + 
					"	d.Id As DepartmentId,\r\n" + 
					"	d.Name AS DepartmentName\r\n" + 
					"FROM seller s \r\n" + 
					"	INNER JOIN department d\r\n" + 
					"	ON s.DepartmentId = d.Id\r\n" + 
					"ORDER BY s.Name");
						
			rs = st.executeQuery();
			
			List<Seller> sellers = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();
			
			while (rs.next()) {
				
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				if (dep == null) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				sellers.add(instantiateSeller(rs, dep));
			}
			return sellers;
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}
	
	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement(
					"SELECT\r\n" + 
					"	s.Id,\r\n" + 
					"	s.Name,\r\n" + 
					"	s.Email,\r\n" + 
					"	s.BirthDate,\r\n" + 
					"	s.BaseSalary,\r\n" + 
					"	d.Id As DepartmentId,\r\n" + 
					"	d.Name AS DepartmentName\r\n" + 
					"FROM seller s \r\n" + 
					"	INNER JOIN department d\r\n" + 
					"	ON s.DepartmentId = d.Id\r\n" + 
					"WHERE s.DepartmentId = ?\r\n" + 
					"ORDER BY s.Name");
			
			st.setInt(1, department.getId());
			
			rs = st.executeQuery();
			
			List<Seller> sellers = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();
			
			while (rs.next()) {
				
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				if (dep == null) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				sellers.add(instantiateSeller(rs, dep));
			}
			return sellers;
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Seller instantiateSeller(ResultSet rs, Department department) throws SQLException {
		Seller seller = new Seller();
		seller.setId(rs.getInt("Id"));
		seller.setName(rs.getString("Name"));
		seller.setEmail(rs.getString("Email"));
		seller.setBirthDate(rs.getDate("BirthDate"));
		seller.setBaseSalary(rs.getDouble("BaseSalary"));
		seller.setDepartment(department);
		return seller;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department department = new Department();
		department.setId(rs.getInt("DepartmentId"));
		department.setName(rs.getString("DepartmentName"));
		return department;
	}

}
